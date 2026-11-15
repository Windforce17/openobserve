// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

//! Opt-in process diagnostics. Initialize before creating application threads.
//!
//! Raw unwinding is best effort, NOT POSIX async-signal-safe: libgcc may take
//! loader/unwinder locks even after warming it up. The fault header and saved
//! instruction pointer are written first without the unwinder. No symbolization,
//! Rust logging, heap buffers or backtrace-crate mutex is used in the handler.

use std::sync::atomic::{AtomicBool, Ordering};

static FATAL: AtomicBool = AtomicBool::new(false);
static MERGE_START: AtomicBool = AtomicBool::new(false);

fn switch(name: &str) -> Result<bool, String> {
    match std::env::var(name) {
        Ok(value) => match value.as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(format!("{name} must be true or false, got {value:?}")),
        },
        Err(std::env::VarError::NotPresent) => Ok(false),
        Err(error) => Err(format!("{name}: {error}")),
    }
}

/// Read process environment (not the later CLI/.env configuration). Both
/// switches default off. Invalid values fail startup, including on non-Linux.
pub fn init_from_env() -> Result<(), String> {
    let fatal = switch("ZO_FATAL_SIGNAL_BACKTRACE")?;
    let merge = switch("ZO_COMPACT_MERGE_START_LOG")?;
    if fatal {
        #[cfg(all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64")
        ))]
        linux::install().map_err(|error| format!("ZO_FATAL_SIGNAL_BACKTRACE: {error}"))?;
        #[cfg(not(all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64")
        )))]
        return Err("ZO_FATAL_SIGNAL_BACKTRACE is supported only on Linux x86_64/aarch64".into());
    }
    FATAL.store(fatal, Ordering::Release);
    MERGE_START.store(merge, Ordering::Relaxed);
    Ok(())
}

pub fn merge_start_enabled() -> bool {
    MERGE_START.load(Ordering::Relaxed)
}

pub fn fatal_signal_enabled() -> bool {
    FATAL.load(Ordering::Acquire)
}

/// Prepare the calling thread before application work, not from a signal handler.
/// Rust's original alternate stack is restored when this thread exits.
pub fn prepare_current_thread() -> std::io::Result<()> {
    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    if FATAL.load(Ordering::Acquire) {
        linux::prepare_thread()?;
    }
    Ok(())
}

/// Linux kernel thread ID, matching the fatal header (not Rust's ThreadId).
pub fn current_thread_id() -> u64 {
    #[cfg(target_os = "linux")]
    {
        unsafe { libc::syscall(libc::SYS_gettid) as u64 }
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod linux {
    use std::{
        cell::RefCell,
        io, mem, ptr,
        sync::{
            OnceLock,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };

    const SIGNALS: [libc::c_int; 5] = [
        libc::SIGSEGV,
        libc::SIGBUS,
        libc::SIGILL,
        libc::SIGFPE,
        libc::SIGABRT,
    ];
    const MAX_FRAMES: usize = 64;
    // The kernel signal frame alone consumes several KiB on modern machines.
    // Never call libgcc on Rust's original (commonly 8 KiB) alternate stack.
    const ALT_STACK_SIZE: usize = 64 * 1024;
    thread_local! {
        static ALT_STACK: RefCell<Option<AltStack>> = const { RefCell::new(None) };
    }

    struct AltStack {
        mapping: *mut libc::c_void,
        mapping_len: usize,
        stack: *mut libc::c_void,
        previous: libc::stack_t,
    }

    impl Drop for AltStack {
        fn drop(&mut self) {
            // SAFETY: this is thread-local storage, dropped on its owner thread.
            // Leak rather than unmap a stack that the kernel may still be using.
            unsafe {
                let mut current: libc::stack_t = mem::zeroed();
                if libc::sigaltstack(ptr::null(), &mut current) == 0
                    && current.ss_flags & libc::SS_ONSTACK == 0
                    && (current.ss_flags & libc::SS_DISABLE != 0
                        || (current.ss_sp == self.stack
                            && libc::sigaltstack(&self.previous, ptr::null_mut()) == 0))
                {
                    // Rust may already have disabled/unmapped its original
                    // stack before TLS destructors run. Do not restore it then.
                    libc::munmap(self.mapping, self.mapping_len);
                }
            }
        }
    }

    pub(super) fn prepare_thread() -> io::Result<()> {
        ALT_STACK.with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot.is_some() {
                return Ok(());
            }
            // SAFETY: startup/thread-start only, outside signal context. Two
            // inaccessible guard pages bound the read/write alternate stack.
            unsafe {
                let mut previous: libc::stack_t = mem::zeroed();
                if libc::sigaltstack(ptr::null(), &mut previous) != 0 {
                    return Err(io::Error::last_os_error());
                }
                if previous.ss_flags & libc::SS_DISABLE == 0 && previous.ss_size >= ALT_STACK_SIZE {
                    return Ok(());
                }
                let page = libc::sysconf(libc::_SC_PAGESIZE);
                if page <= 0 {
                    return Err(io::Error::last_os_error());
                }
                let page = page as usize;
                let mapping_len = ALT_STACK_SIZE + 2 * page;
                let mapping = libc::mmap(
                    ptr::null_mut(),
                    mapping_len,
                    libc::PROT_NONE,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                    -1,
                    0,
                );
                if mapping == libc::MAP_FAILED {
                    return Err(io::Error::last_os_error());
                }
                let stack = mapping.cast::<u8>().add(page).cast();
                let alternate = libc::stack_t {
                    ss_sp: stack,
                    ss_flags: 0,
                    ss_size: ALT_STACK_SIZE,
                };
                if libc::mprotect(stack, ALT_STACK_SIZE, libc::PROT_READ | libc::PROT_WRITE) != 0
                    || libc::sigaltstack(&alternate, ptr::null_mut()) != 0
                {
                    let error = io::Error::last_os_error();
                    libc::munmap(mapping, mapping_len);
                    return Err(error);
                }
                *slot = Some(AltStack {
                    mapping,
                    mapping_len,
                    stack,
                    previous,
                });
            }
            Ok(())
        })
    }

    static PREVIOUS: [AtomicUsize; 5] = [const { AtomicUsize::new(0) }; 5];
    static PREVIOUS_FLAGS: [AtomicUsize; 5] = [const { AtomicUsize::new(0) }; 5];
    static BASE: AtomicUsize = AtomicUsize::new(0);
    static ACTIVE: AtomicBool = AtomicBool::new(false);
    static INSTALLED: OnceLock<Result<(), String>> = OnceLock::new();

    pub(super) fn install() -> Result<(), String> {
        INSTALLED
            .get_or_init(|| {
                prepare_thread().map_err(|error| error.to_string())?;
                // Force lazy unwinder loading outside signal context. This reduces,
                // but cannot eliminate, loader-lock and allocator risks in libgcc.
                unsafe { backtrace::trace_unsynchronized(|_| true) };
                unsafe extern "C" fn image(
                    info: *mut libc::dl_phdr_info,
                    _: usize,
                    _: *mut libc::c_void,
                ) -> libc::c_int {
                    // SAFETY: dl_iterate_phdr supplies a valid descriptor and name.
                    let info = unsafe { &*info };
                    if !info.dlpi_name.is_null() && unsafe { *info.dlpi_name } == 0 {
                        BASE.store(info.dlpi_addr as usize, Ordering::Relaxed);
                        return 1;
                    }
                    0
                }
                // SAFETY: callback only reads loader-owned data during startup.
                unsafe { libc::dl_iterate_phdr(Some(image), ptr::null_mut()) };
                for (index, signal) in SIGNALS.into_iter().enumerate() {
                    // SAFETY: zero is a valid initial sigaction; kernel fills old.
                    let mut old: libc::sigaction = unsafe { mem::zeroed() };
                    if unsafe { libc::sigaction(signal, ptr::null(), &mut old) } != 0 {
                        return Err(io::Error::last_os_error().to_string());
                    }
                    PREVIOUS[index].store(old.sa_sigaction, Ordering::Relaxed);
                    PREVIOUS_FLAGS[index].store(old.sa_flags as usize, Ordering::Release);
                    let mut action: libc::sigaction = unsafe { mem::zeroed() };
                    action.sa_flags = libc::SA_SIGINFO | libc::SA_ONSTACK;
                    action.sa_sigaction = handler as *const () as usize;
                    // Only the current signal is masked. In particular SIGABRT must
                    // remain available when Rust's stack-overflow handler aborts.
                    if unsafe { libc::sigaction(signal, &action, ptr::null_mut()) } != 0 {
                        return Err(io::Error::last_os_error().to_string());
                    }
                }
                Ok(())
            })
            .clone()
    }

    #[test]
    fn alternate_stack_is_guarded_idempotent_and_restored() {
        std::thread::spawn(|| unsafe {
            let mut original: libc::stack_t = mem::zeroed();
            assert_eq!(libc::sigaltstack(ptr::null(), &mut original), 0);
            prepare_thread().unwrap();
            let mut prepared: libc::stack_t = mem::zeroed();
            assert_eq!(libc::sigaltstack(ptr::null(), &mut prepared), 0);
            assert!(prepared.ss_size >= ALT_STACK_SIZE);
            prepare_thread().unwrap();
            let mut repeated: libc::stack_t = mem::zeroed();
            assert_eq!(libc::sigaltstack(ptr::null(), &mut repeated), 0);
            assert_eq!(prepared.ss_sp, repeated.ss_sp);
            ALT_STACK.with(|slot| {
                let guard = slot.borrow_mut().take().unwrap();
                let mapping = guard.mapping as usize;
                let maps = std::fs::read_to_string("/proc/self/maps").unwrap();
                assert!(maps.lines().any(|line| {
                    line.starts_with(&format!("{mapping:x}-")) && line.contains("---p")
                }));
                drop(guard);
            });
            let mut restored: libc::stack_t = mem::zeroed();
            assert_eq!(libc::sigaltstack(ptr::null(), &mut restored), 0);
            assert_eq!(original.ss_sp, restored.ss_sp);
            assert_eq!(original.ss_size, restored.ss_size);
            assert_eq!(original.ss_flags, restored.ss_flags);
        })
        .join()
        .unwrap();
    }

    #[test]
    fn alternate_stack_mapping_is_released_on_thread_exit() {
        let mapping = std::thread::spawn(|| {
            prepare_thread().unwrap();
            ALT_STACK.with(|slot| slot.borrow().as_ref().unwrap().mapping as usize)
        })
        .join()
        .unwrap();
        let mut resident = 0u8;
        assert_eq!(
            unsafe { libc::mincore(mapping as *mut libc::c_void, 1, &mut resident) },
            -1
        );
        assert_eq!(
            io::Error::last_os_error().raw_os_error(),
            Some(libc::ENOMEM)
        );
    }

    struct Line {
        bytes: [u8; 256],
        len: usize,
    }

    impl Line {
        fn new() -> Self {
            Self {
                bytes: [0; 256],
                len: 0,
            }
        }
        fn text(&mut self, text: &[u8]) {
            for &byte in text {
                if self.len < self.bytes.len() {
                    self.bytes[self.len] = byte;
                    self.len += 1;
                }
            }
        }
        fn number(&mut self, mut value: usize, radix: usize) {
            let mut digits = [0u8; 32];
            let mut pos = digits.len();
            loop {
                pos -= 1;
                digits[pos] = b"0123456789abcdef"[value % radix];
                value /= radix;
                if value == 0 {
                    break;
                }
            }
            self.text(&digits[pos..]);
        }
        fn hex(&mut self, value: usize) {
            self.text(b"0x");
            self.number(value, 16);
        }
        fn write(&mut self) {
            self.text(b"\n");
            let mut offset = 0;
            while offset < self.len {
                // SAFETY: stack buffer remains valid throughout write(2).
                let written = unsafe {
                    libc::write(
                        libc::STDERR_FILENO,
                        self.bytes[offset..self.len].as_ptr().cast(),
                        self.len - offset,
                    )
                };
                if written > 0 {
                    offset += written as usize;
                } else if written < 0 && unsafe { *libc::__errno_location() } == libc::EINTR {
                    continue;
                } else {
                    break;
                }
            }
            self.len = 0;
        }
    }

    unsafe extern "C" fn handler(
        signal: libc::c_int,
        info: *mut libc::siginfo_t,
        context: *mut libc::c_void,
    ) {
        // SAFETY: sigaction delivers valid siginfo/ucontext pointers on Linux.
        unsafe {
            let saved_errno = *libc::__errno_location();
            let mut default: libc::sigaction = mem::zeroed();
            default.sa_sigaction = libc::SIG_DFL;
            libc::sigaction(signal, &default, ptr::null_mut());
            if !ACTIVE.swap(true, Ordering::AcqRel) {
                emit(signal, &*info, &*context.cast::<libc::ucontext_t>());
            }
            // Preserve Rust's guard-page detection and its SIGABRT outcome.
            // Call the original handler only after diagnostic stack frames have
            // returned, leaving the alternate stack available to the runtime.
            if let Some(index) = SIGNALS.iter().position(|&value| value == signal) {
                let flags = PREVIOUS_FLAGS[index].load(Ordering::Acquire);
                let previous = PREVIOUS[index].load(Ordering::Relaxed);
                if previous != libc::SIG_DFL && previous != libc::SIG_IGN {
                    // Rust aborts from its overflow handler. Do not deliver a
                    // second userspace signal frame on the small alternate
                    // stack: the SIGSEGV/SIGBUS diagnostic is already complete.
                    if signal == libc::SIGSEGV || signal == libc::SIGBUS {
                        libc::sigaction(libc::SIGABRT, &default, ptr::null_mut());
                    }
                    if flags & libc::SA_SIGINFO as usize != 0 {
                        let previous: unsafe extern "C" fn(
                            libc::c_int,
                            *mut libc::siginfo_t,
                            *mut libc::c_void,
                        ) = mem::transmute(previous);
                        previous(signal, info, context);
                    } else {
                        let previous: unsafe extern "C" fn(libc::c_int) = mem::transmute(previous);
                        previous(signal);
                    }
                }
            }
            libc::sigaction(signal, &default, ptr::null_mut());
            // Hardware faults retry the faulting instruction after sigreturn,
            // preserving the kernel's original address/core context. A raised
            // signal has no instruction to retry: queue it to THIS thread.
            if (*info).si_code <= 0 {
                libc::syscall(
                    libc::SYS_tgkill,
                    libc::getpid(),
                    libc::syscall(libc::SYS_gettid),
                    signal,
                );
            }
            *libc::__errno_location() = saved_errno;
        }
    }

    #[inline(never)]
    unsafe fn emit(signal: libc::c_int, info: &libc::siginfo_t, context: &libc::ucontext_t) {
        let tid = unsafe { libc::syscall(libc::SYS_gettid) } as usize;
        let mut name = [0u8; 16];
        // PR_GET_NAME reads the kernel's comm (15 bytes, possibly truncated),
        // without pthread/TLS initialization or the Rust thread-name lock.
        unsafe {
            libc::syscall(
                libc::SYS_prctl,
                libc::PR_GET_NAME,
                name.as_mut_ptr(),
                0,
                0,
                0,
            )
        };
        let mut alt: libc::stack_t = unsafe { mem::zeroed() };
        unsafe { libc::sigaltstack(ptr::null(), &mut alt) };
        let mut line = Line::new();
        line.text(b"O2_FATAL signal=");
        line.number(signal as usize, 10);
        line.text(b" si_code=");
        if info.si_code < 0 {
            line.text(b"-");
        }
        line.number(info.si_code.unsigned_abs() as usize, 10);
        line.text(b" tid=");
        line.number(tid, 10);
        line.text(b" thread=");
        for byte in name.into_iter().take_while(|&byte| byte != 0) {
            line.text(&[if byte.is_ascii_graphic() { byte } else { b'_' }]);
        }
        line.text(b" si_addr=");
        // si_addr is only meaningful for kernel-generated fault signals; its
        // union overlaps the sender PID for SI_USER/SI_TKILL (e.g. abort).
        line.hex(if info.si_code > 0 {
            (unsafe { info.si_addr() }) as usize
        } else {
            0
        });
        line.text(b" exe_base=");
        line.hex(BASE.load(Ordering::Relaxed));
        line.text(b" altstack=");
        line.number(usize::from(alt.ss_flags & libc::SS_ONSTACK != 0), 10);
        line.text(b" altstack_size=");
        line.number(alt.ss_size, 10);
        line.write();
        #[cfg(target_arch = "x86_64")]
        let pc = context.uc_mcontext.gregs[libc::REG_RIP as usize] as usize;
        #[cfg(target_arch = "aarch64")]
        let pc = context.uc_mcontext.pc as usize;
        line.text(b"O2_FATAL tid=");
        line.number(tid, 10);
        line.text(b" fault_pc=");
        line.hex(pc);
        line.write();
        let mut count = 0;
        // SAFETY: Linux uses _Unwind_Backtrace here (no backtrace crate mutex).
        // Only raw IPs are read; no Frame clone or symbol resolver is called.
        // This remains best effort if libgcc is interrupted while holding a lock.
        if alt.ss_flags & libc::SS_ONSTACK != 0 && alt.ss_size >= ALT_STACK_SIZE {
            unsafe {
                backtrace::trace_unsynchronized(|frame| {
                    line.text(b"O2_FATAL tid=");
                    line.number(tid, 10);
                    line.text(b" frame=");
                    line.number(count, 10);
                    line.text(b" ip=");
                    line.hex(frame.ip() as usize);
                    line.write();
                    count += 1;
                    count < MAX_FRAMES
                })
            };
        } else {
            // A foreign thread may not have run prepare_current_thread. Keep the
            // saved PC above, but do not risk unwinding on a tiny/absent altstack.
            line.text(b"O2_FATAL tid=");
            line.number(tid, 10);
            line.text(b" unwind=skipped_small_altstack");
            line.write();
        }
        line.text(b"O2_FATAL tid=");
        line.number(tid, 10);
        line.text(b" end frames=");
        line.number(count, 10);
        line.write();
    }
}

#[cfg(all(
    test,
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod tests {
    use std::{
        os::unix::process::ExitStatusExt,
        process::{Command, Output, Stdio},
        time::{Duration, Instant},
    };

    fn child(mode: &str, enabled: Option<&str>) -> Output {
        configured_child(mode, "ZO_FATAL_SIGNAL_BACKTRACE", enabled)
    }

    fn configured_child(mode: &str, variable: &str, enabled: Option<&str>) -> Output {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "diagnostics::tests::fatal_child", "--nocapture"])
            .env("O2_FATAL_TEST_MODE", mode)
            .env_remove("ZO_COMPACT_MERGE_START_LOG")
            .env_remove("ZO_FATAL_SIGNAL_BACKTRACE");
        if let Some(value) = enabled {
            command.env(variable, value);
        }
        let mut process = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let start = Instant::now();
        while process.try_wait().unwrap().is_none() {
            if start.elapsed() > Duration::from_secs(10) {
                process.kill().unwrap();
                let output = process.wait_with_output().unwrap();
                panic!("fatal diagnostic child timed out: {output:?}");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        process.wait_with_output().unwrap()
    }

    #[test]
    fn switches_are_strict_and_default_off() {
        for variable in ["ZO_FATAL_SIGNAL_BACKTRACE", "ZO_COMPACT_MERGE_START_LOG"] {
            for value in [None, Some("false"), Some("true")] {
                let output = configured_child("config", variable, value);
                assert!(output.status.success(), "{output:?}");
            }
            for value in ["", "1", "TRUE", "false ", "garbage"] {
                let output = configured_child("config", variable, Some(value));
                assert!(!output.status.success(), "accepted {variable}={value:?}");
                assert!(String::from_utf8_lossy(&output.stderr).contains("must be true or false"));
            }
        }
    }

    #[test]
    fn fatal_signals_preserve_exit_and_stack_overflow_message() {
        for (mode, signal) in [
            ("segv", libc::SIGSEGV),
            ("bus", libc::SIGBUS),
            ("ill", libc::SIGILL),
            ("fpe", libc::SIGFPE),
            ("abort", libc::SIGABRT),
            ("overflow", libc::SIGABRT),
        ] {
            let baseline = child(mode, None);
            let disabled = child(mode, Some("false"));
            let enabled = child(mode, Some("true"));
            assert_eq!(
                baseline.status.signal(),
                Some(signal),
                "baseline {mode}: {baseline:?}"
            );
            assert_eq!(
                disabled.status.signal(),
                baseline.status.signal(),
                "disabled {mode}"
            );
            assert_eq!(
                enabled.status.signal(),
                baseline.status.signal(),
                "enabled {mode}: {enabled:?}"
            );
            assert!(!String::from_utf8_lossy(&baseline.stderr).contains("O2_FATAL"));
            assert!(!String::from_utf8_lossy(&disabled.stderr).contains("O2_FATAL"));
            let stderr = String::from_utf8_lossy(&enabled.stderr);
            assert!(stderr.contains("O2_FATAL signal="), "{mode}: {stderr}");
            assert!(stderr.contains("thread=job_runtime"), "{mode}: {stderr}");
            assert!(
                stderr.contains("altstack=1 altstack_size=65536"),
                "{mode}: {stderr}"
            );
            assert!(stderr.contains("exe_base=0x"), "{mode}: {stderr}");
            assert!(stderr.contains("fault_pc=0x"), "{mode}: {stderr}");
            assert!(stderr.matches(" ip=0x").count() >= 4, "{mode}: {stderr}");
            assert!(stderr.contains("end frames="), "{mode}: {stderr}");
            if mode == "overflow" {
                for output in [&baseline, &disabled, &enabled] {
                    assert!(
                        String::from_utf8_lossy(&output.stderr)
                            .contains("has overflowed its stack"),
                        "{output:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn foreign_thread_keeps_fault_pc_without_unwinding_small_stack() {
        let output = child("unprepared", Some("true"));
        assert_eq!(output.status.signal(), Some(libc::SIGSEGV));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("fault_pc=0x"), "{stderr}");
        assert!(stderr.contains("unwind=skipped_small_altstack"), "{stderr}");
        assert!(stderr.contains("end frames=0"), "{stderr}");
    }

    #[test]
    fn fatal_child() {
        let Ok(mode) = std::env::var("O2_FATAL_TEST_MODE") else {
            return;
        };
        // Never write a core from a deliberate local fault.
        let limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_CORE, &limit) }, 0);
        super::init_from_env().unwrap();
        if mode == "config" {
            assert_eq!(
                super::fatal_signal_enabled(),
                std::env::var("ZO_FATAL_SIGNAL_BACKTRACE").is_ok_and(|value| value == "true")
            );
            assert_eq!(
                super::merge_start_enabled(),
                std::env::var("ZO_COMPACT_MERGE_START_LOG").is_ok_and(|value| value == "true")
            );
            super::init_from_env().unwrap();
            return;
        }
        std::thread::Builder::new()
            .name("job_runtime".into())
            .stack_size(256 * 1024)
            .spawn(move || {
                if mode != "unprepared" {
                    super::prepare_current_thread().unwrap();
                }
                crash(if mode == "unprepared" { "segv" } else { &mode });
            })
            .unwrap()
            .join()
            .unwrap();
        panic!("fault returned");
    }

    #[inline(never)]
    fn crash(mode: &str) {
        unsafe {
            match mode {
                "segv" => {
                    let page = libc::mmap(
                        std::ptr::null_mut(),
                        4096,
                        libc::PROT_NONE,
                        libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                        -1,
                        0,
                    );
                    assert_ne!(page, libc::MAP_FAILED);
                    std::ptr::read_volatile(page.cast::<u8>());
                }
                "bus" => {
                    let fd = libc::memfd_create(c"fatal-test".as_ptr(), 0);
                    assert!(fd >= 0);
                    let page = libc::mmap(
                        std::ptr::null_mut(),
                        4096,
                        libc::PROT_READ,
                        libc::MAP_SHARED,
                        fd,
                        0,
                    );
                    assert_ne!(page, libc::MAP_FAILED);
                    std::ptr::read_volatile(page.cast::<u8>());
                }
                "ill" => {
                    #[cfg(target_arch = "x86_64")]
                    std::arch::asm!("ud2", options(noreturn));
                    #[cfg(target_arch = "aarch64")]
                    std::arch::asm!("udf #0", options(noreturn));
                }
                "fpe" => {
                    #[cfg(target_arch = "x86_64")]
                    std::arch::asm!("xor edx, edx", "mov eax, 1", "xor ecx, ecx", "div ecx", out("eax") _, out("ecx") _, out("edx") _);
                    // AArch64 integer division by zero does not trap.
                    #[cfg(target_arch = "aarch64")]
                    libc::raise(libc::SIGFPE);
                }
                "abort" => libc::abort(),
                "overflow" => overflow(1),
                _ => panic!("unknown fault mode"),
            }
        }
    }

    #[inline(never)]
    fn overflow(depth: usize) {
        let data = std::hint::black_box([depth; 128]);
        if std::hint::black_box(depth) != 0 {
            overflow(depth.wrapping_add(1));
        }
        std::hint::black_box(data);
    }
}
