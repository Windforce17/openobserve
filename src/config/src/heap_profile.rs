// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! M27: opt-in SAMPLING HEAP PROFILER — direct allocation-site attribution of
//! LIVE heap bytes on a running binary, wrapped around the real global
//! allocator (mimalloc stays underneath, untouched).
//!
//! Activation contract
//! - `ZO_HEAP_PROFILE_SAMPLE_EVERY_MB` unset/`0`/unparseable => compiled in but FULLY INERT: the
//!   alloc and dealloc hot paths are a single relaxed atomic load + branch each. `init()` must be
//!   called once early in `main` (activation never changes afterwards); allocations before `init()`
//!   are simply not attributed.
//! - `=N` (MB) => size-weighted sampling: every allocation's size is added to one global
//!   accumulator; each time the accumulator crosses a multiple of N MiB the crossing allocation is
//!   sampled (expected one sample per N MiB of allocation flow, so a call site's sampling
//!   probability is proportional to the bytes it allocates). A sample records the raw backtrace (<=
//!   64 frame IPs, FNV-hashed for dedup) and the pointer; the pointer's record is dropped when that
//!   pointer is freed, so per-stack sample sets track LIVE bytes, not allocation churn.
//! - `ZO_HEAP_PROFILE_REPORT_SECS` (default 60): a background thread started on activation logs the
//!   top [`REPORT_TOP`] stacks by estimated live bytes. Estimator: a sample taken for `k`
//!   accumulator crossings carries weight `k * N MiB` (unbiased for sizes both below and above the
//!   sampling interval); a stack's `live_est` is the sum of its live sample weights.
//!
//! Safety / overhead design
//! - Re-entrancy: everything past the hot-path checks is guarded by a thread-local flag.
//!   Profiler-internal allocations (map growth, backtrace machinery, symbolization, log formatting)
//!   pass straight through to the inner allocator and are never sampled, which also makes lock
//!   recursion (shard/stack mutexes) impossible.
//! - Dealloc pre-filter: a 2^20-bit membership bitmap (128 KiB, never cleared) keyed by pointer
//!   value. When active, a free that was never sampled costs one relaxed load + mask; only filter
//!   hits (real samples + rare aliases; pointer reuse by the allocator bounds saturation) take the
//!   shard lock. Dealloc accounting runs BEFORE the inner free so a recycled address can never be
//!   double-attributed.
//! - Symbol resolution is lazy: raw IPs are stored at sample time; names are resolved (via the
//!   `backtrace` crate, the engine under `std::backtrace`) only in the reporter thread. The release
//!   profile must keep the symbol table (`strip = "debuginfo"`) for names to resolve.

use std::{
    alloc::{GlobalAlloc, Layout},
    cell::Cell,
    collections::HashMap,
    ffi::c_void,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

/// Maximum raw frames captured per sampled allocation.
const MAX_FRAMES: usize = 64;
/// Frames printed per stack in the periodic report.
const REPORT_FRAMES: usize = 8;
/// Stacks printed per periodic report.
const REPORT_TOP: usize = 15;
/// Longest printed symbol name; longer ones are truncated.
const MAX_FRAME_CHARS: usize = 120;
/// Shards of the live sampled-pointer map (locked only on sample/unsample).
const PTR_SHARDS: usize = 64;
/// Words of the dealloc pre-filter bitmap: 16384 x u64 = 2^20 bits (128 KiB).
const FILTER_WORDS: usize = 1 << 14;
/// Dead stack entries are evicted when the registry grows past this.
const MAX_TRACKED_STACKS: usize = 32_768;

/// Sampling interval in bytes; `0` = profiler off (the whole hot path).
static SAMPLE_EVERY_BYTES: AtomicU64 = AtomicU64::new(0);
/// Total allocation flow (bytes) since activation; drives sampling decisions.
static ALLOC_FLOW_BYTES: AtomicU64 = AtomicU64::new(0);
static SAMPLES_TAKEN: AtomicU64 = AtomicU64::new(0);
static SAMPLES_LIVE: AtomicU64 = AtomicU64::new(0);
/// Samples skipped because the crossing happened inside the profiler itself.
static REENTRANT_SKIPS: AtomicU64 = AtomicU64::new(0);

/// Membership pre-filter over sampled pointer values (bits are only ever
/// set; allocator address reuse keeps distinct sampled addresses — and thus
/// saturation — bounded; a saturated filter degrades to a map lookup per
/// free, never to a wrong answer).
static FILTER: [AtomicU64; FILTER_WORDS] = [const { AtomicU64::new(0) }; FILTER_WORDS];

/// One live sampled allocation.
struct PtrRec {
    stack_hash: u64,
    /// Estimated bytes this sample stands for (crossings x interval).
    weight: u64,
    /// The sampled allocation's own size.
    size: u64,
}

static LIVE_PTRS: [Mutex<Option<HashMap<usize, PtrRec>>>; PTR_SHARDS] =
    [const { Mutex::new(None) }; PTR_SHARDS];

/// Aggregate for one distinct call stack.
struct StackRec {
    frames: [usize; MAX_FRAMES],
    depth: u16,
    live_weight: u64,
    live_size: u64,
    live_count: u64,
    total_count: u64,
}

static STACKS: Mutex<Option<HashMap<u64, StackRec>>> = Mutex::new(None);

thread_local! {
    /// True while this thread is inside profiler bookkeeping.
    static IN_PROFILER: Cell<bool> = const { Cell::new(false) };
}

/// RAII re-entrancy guard; `enter()` fails if the thread is already inside
/// the profiler (or its TLS is unavailable during thread teardown).
struct ReentrancyGuard;

impl ReentrancyGuard {
    fn enter() -> Option<Self> {
        let entered = IN_PROFILER
            .try_with(|flag| {
                if flag.get() {
                    false
                } else {
                    flag.set(true);
                    true
                }
            })
            .unwrap_or(false);
        // NB: an explicit branch, never `entered.then_some(ReentrancyGuard)`
        // — then_some constructs its argument eagerly, and on the blocked
        // path dropping that never-issued guard would clear the flag the
        // OUTER guard still owns (self-sampling + shard-lock recursion).
        if entered { Some(ReentrancyGuard) } else { None }
    }
}

impl Drop for ReentrancyGuard {
    fn drop(&mut self) {
        let _ = IN_PROFILER.try_with(|flag| flag.set(false));
    }
}

// ---------------------------------------------------------------------------
// the allocator wrapper
// ---------------------------------------------------------------------------

/// Zero-cost-when-off sampling wrapper around the process global allocator.
/// The inner allocator (mimalloc in prod) handles every byte; the wrapper
/// only observes.
pub struct HeapProfileAlloc<A> {
    inner: A,
}

impl<A> HeapProfileAlloc<A> {
    pub const fn new(inner: A) -> Self {
        Self { inner }
    }
}

unsafe impl<A: GlobalAlloc> GlobalAlloc for HeapProfileAlloc<A> {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.inner.alloc(layout) };
        on_alloc(ptr, layout.size());
        ptr
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.inner.alloc_zeroed(layout) };
        on_alloc(ptr, layout.size());
        ptr
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // account BEFORE the inner free: once freed, another thread may get
        // this exact address back and sample it — removing after would then
        // drop the NEW record.
        on_dealloc(ptr);
        unsafe { self.inner.dealloc(ptr, layout) }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // realloc = free(old) + alloc(new) for attribution. Unsampling the
        // old pointer before the inner call keeps the address-reuse
        // invariant; if the realloc then fails the old record is already
        // gone (a rare undercount, never a corruption).
        on_dealloc(ptr);
        let new_ptr = unsafe { self.inner.realloc(ptr, layout, new_size) };
        on_alloc(new_ptr, new_size);
        new_ptr
    }
}

// ---------------------------------------------------------------------------
// hot paths
// ---------------------------------------------------------------------------

#[inline(always)]
fn on_alloc(ptr: *mut u8, size: usize) {
    let every = SAMPLE_EVERY_BYTES.load(Ordering::Relaxed);
    if every == 0 || ptr.is_null() {
        return;
    }
    let prev = ALLOC_FLOW_BYTES.fetch_add(size as u64, Ordering::Relaxed);
    let crossings = interval_crossings(prev, size as u64, every);
    if crossings == 0 {
        return;
    }
    sample_allocation(ptr as usize, size as u64, crossings.saturating_mul(every));
}

#[inline(always)]
fn on_dealloc(ptr: *mut u8) {
    if SAMPLE_EVERY_BYTES.load(Ordering::Relaxed) == 0 {
        return;
    }
    let ptr = ptr as usize;
    if !filter_maybe_contains(ptr) {
        return;
    }
    unsample_allocation(ptr);
}

/// How many multiples of `every` the accumulator crossed going from `prev`
/// to `prev + size`. Expected value is `size / every`, so weighting each
/// sample by `crossings * every` estimates allocation flow without bias for
/// sizes below or above the interval.
#[inline(always)]
fn interval_crossings(prev: u64, size: u64, every: u64) -> u64 {
    (prev + size) / every - prev / every
}

// ---------------------------------------------------------------------------
// pointer hashing: pre-filter + shard selection
// ---------------------------------------------------------------------------

#[inline(always)]
fn mix_ptr(ptr: usize) -> u64 {
    (ptr as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

#[inline(always)]
fn filter_slot(ptr: usize) -> (usize, u64) {
    let bit = (mix_ptr(ptr) >> 44) as usize; // top 20 bits -> 2^20 positions
    (bit >> 6, 1u64 << (bit & 63))
}

#[inline(always)]
fn filter_maybe_contains(ptr: usize) -> bool {
    let (word, mask) = filter_slot(ptr);
    FILTER[word].load(Ordering::Relaxed) & mask != 0
}

fn filter_insert(ptr: usize) {
    let (word, mask) = filter_slot(ptr);
    FILTER[word].fetch_or(mask, Ordering::Relaxed);
}

#[inline(always)]
fn ptr_shard(ptr: usize) -> usize {
    ((mix_ptr(ptr) >> 38) & (PTR_SHARDS as u64 - 1)) as usize
}

fn hash_frames(frames: &[usize]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for &frame in frames {
        hash ^= frame as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    hash
}

// ---------------------------------------------------------------------------
// cold paths: sample / unsample
// ---------------------------------------------------------------------------

#[cold]
#[inline(never)]
fn sample_allocation(ptr: usize, size: u64, weight: u64) {
    let Some(_guard) = ReentrancyGuard::enter() else {
        // the crossing was consumed by a profiler-internal allocation
        REENTRANT_SKIPS.fetch_add(1, Ordering::Relaxed);
        return;
    };
    // the guard also fences off panics: an unwind out of a GlobalAlloc impl
    // is UB, so swallow anything unexpected here.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        record_sample(ptr, size, weight);
    }));
}

fn record_sample(ptr: usize, size: u64, weight: u64) {
    let mut frames = [0usize; MAX_FRAMES];
    let mut depth = 0usize;
    backtrace::trace(|frame| {
        frames[depth] = frame.ip() as usize;
        depth += 1;
        depth < MAX_FRAMES
    });
    if depth == 0 {
        return;
    }
    let stack_hash = hash_frames(&frames[..depth]);

    let evicted = {
        let mut shard = LIVE_PTRS[ptr_shard(ptr)]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        shard.get_or_insert_default().insert(
            ptr,
            PtrRec {
                stack_hash,
                weight,
                size,
            },
        )
    };
    filter_insert(ptr);

    {
        let mut stacks = STACKS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let map = stacks.get_or_insert_default();
        // a same-address record can only linger if its free was skipped
        // during thread teardown; treat it as freed now
        if let Some(old) = evicted {
            SAMPLES_LIVE.fetch_sub(1, Ordering::Relaxed);
            if let Some(rec) = map.get_mut(&old.stack_hash) {
                rec.live_weight = rec.live_weight.saturating_sub(old.weight);
                rec.live_size = rec.live_size.saturating_sub(old.size);
                rec.live_count = rec.live_count.saturating_sub(1);
            }
        }
        let rec = map.entry(stack_hash).or_insert_with(|| StackRec {
            frames,
            depth: depth as u16,
            live_weight: 0,
            live_size: 0,
            live_count: 0,
            total_count: 0,
        });
        rec.live_weight += weight;
        rec.live_size += size;
        rec.live_count += 1;
        rec.total_count += 1;
    }
    SAMPLES_TAKEN.fetch_add(1, Ordering::Relaxed);
    SAMPLES_LIVE.fetch_add(1, Ordering::Relaxed);
}

#[cold]
#[inline(never)]
fn unsample_allocation(ptr: usize) {
    // A guarded thread is inside profiler bookkeeping and only frees
    // profiler-internal allocations, which are never sampled — skipping here
    // both avoids shard-lock recursion (a filter alias of a map-growth free
    // would retake the held lock) and stays correct.
    let Some(_guard) = ReentrancyGuard::enter() else {
        return;
    };
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let removed = {
            let mut shard = LIVE_PTRS[ptr_shard(ptr)]
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            shard.as_mut().and_then(|map| map.remove(&ptr))
        };
        let Some(rec) = removed else {
            return; // filter alias, not a sampled pointer
        };
        SAMPLES_LIVE.fetch_sub(1, Ordering::Relaxed);
        let mut stacks = STACKS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(stack) = stacks.as_mut().and_then(|map| map.get_mut(&rec.stack_hash)) {
            stack.live_weight = stack.live_weight.saturating_sub(rec.weight);
            stack.live_size = stack.live_size.saturating_sub(rec.size);
            stack.live_count = stack.live_count.saturating_sub(1);
        }
    }));
}

// ---------------------------------------------------------------------------
// activation + reporter
// ---------------------------------------------------------------------------

/// Read `ZO_HEAP_PROFILE_SAMPLE_EVERY_MB` and, when set to a positive value,
/// activate sampling and start the periodic reporter thread. Call once,
/// early in `main` (idempotent; never touches config machinery so it works
/// before config load). With the env unset this is a no-op and the wrapper
/// stays fully inert.
pub fn init() {
    let raw = std::env::var("ZO_HEAP_PROFILE_SAMPLE_EVERY_MB").ok();
    let sample_every_mb = match raw.as_deref().map(str::trim) {
        None | Some("") => 0,
        Some(value) => value.parse::<u64>().unwrap_or_else(|_| {
            eprintln!(
                "heap-profile: invalid ZO_HEAP_PROFILE_SAMPLE_EVERY_MB={value:?} — staying off"
            );
            0
        }),
    };
    if sample_every_mb == 0 {
        return;
    }
    let report_secs = std::env::var("ZO_HEAP_PROFILE_REPORT_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(60)
        .max(1);
    let sample_every_bytes = sample_every_mb.saturating_mul(1024 * 1024);
    if SAMPLE_EVERY_BYTES.swap(sample_every_bytes, Ordering::Relaxed) != 0 {
        return; // already active
    }
    // stderr as well: activation can precede the log subscriber
    eprintln!("heap-profile: ACTIVE sample_every={sample_every_mb}MB report_every={report_secs}s");
    log::info!("heap-profile: ACTIVE sample_every={sample_every_mb}MB report_every={report_secs}s");
    let _ = std::thread::Builder::new()
        .name("heap-profile".into())
        .spawn(move || {
            let mut seq = 0u64;
            loop {
                std::thread::sleep(std::time::Duration::from_secs(report_secs));
                seq += 1;
                report(seq);
            }
        });
}

fn report(seq: u64) {
    let Some(_guard) = ReentrancyGuard::enter() else {
        return;
    };
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        for line in render_report(seq) {
            log::info!("{line}");
        }
    }));
}

/// Snapshot + format one report. Separated from `report` so tests can assert
/// on content without a live logger.
fn render_report(seq: u64) -> Vec<String> {
    // snapshot live stacks under the lock, then release it before the
    // (allocating, slow) symbol resolution
    let mut live: Vec<(u64, u64, u64, [usize; MAX_FRAMES], u16)> = {
        let mut stacks = STACKS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(map) = stacks.as_mut() else {
            return vec![format!(
                "heap-profile: report#{seq} sample_every={}MB no samples yet",
                SAMPLE_EVERY_BYTES.load(Ordering::Relaxed) / (1024 * 1024)
            )];
        };
        if map.len() > MAX_TRACKED_STACKS {
            map.retain(|_, rec| rec.live_count > 0);
        }
        map.values()
            .filter(|rec| rec.live_count > 0)
            .map(|rec| {
                (
                    rec.live_weight,
                    rec.live_count,
                    rec.live_size,
                    rec.frames,
                    rec.depth,
                )
            })
            .collect()
    };
    let live_est_total: u64 = live.iter().map(|entry| entry.0).sum();
    live.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.0));
    live.truncate(REPORT_TOP);

    let mb = |bytes: u64| bytes as f64 / (1024.0 * 1024.0);
    let mut lines = Vec::with_capacity(live.len() + 1);
    lines.push(format!(
        "heap-profile: report#{seq} sample_every={}MB alloc_flow={:.1}GB live_est_total={:.1}MB \
         samples_live={} samples_taken={} reentrant_skips={}",
        SAMPLE_EVERY_BYTES.load(Ordering::Relaxed) / (1024 * 1024),
        ALLOC_FLOW_BYTES.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0 * 1024.0),
        mb(live_est_total),
        SAMPLES_LIVE.load(Ordering::Relaxed),
        SAMPLES_TAKEN.load(Ordering::Relaxed),
        REENTRANT_SKIPS.load(Ordering::Relaxed),
    ));
    for (rank, &(weight, count, size, frames, depth)) in live.iter().enumerate() {
        let stack = symbolize(&frames[..depth as usize]);
        lines.push(format!(
            "heap-profile: rank={} live_est={:.1}MB count={} avg_sz_kb={:.1} stack=<{}>",
            rank + 1,
            mb(weight),
            count,
            size as f64 / count.max(1) as f64 / 1024.0,
            stack.join(";"),
        ));
    }
    lines
}

/// Resolve up to [`REPORT_FRAMES`] meaningful names from raw frame IPs,
/// dropping profiler/allocator shim frames.
fn symbolize(frames: &[usize]) -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(REPORT_FRAMES);
    for &ip in frames {
        if out.len() >= REPORT_FRAMES {
            break;
        }
        let mut name: Option<String> = None;
        // ip is a return address: resolve one byte back, inside the call
        backtrace::resolve(ip.wrapping_sub(1) as *mut c_void, |symbol| {
            if name.is_none()
                && let Some(sym_name) = symbol.name()
            {
                name = Some(format!("{sym_name:#}"));
            }
        });
        let Some(mut label) = name else {
            out.push(format!("{ip:#x}"));
            continue;
        };
        if is_noise_frame(&label) {
            continue;
        }
        if label.len() > MAX_FRAME_CHARS {
            let cut = (0..=MAX_FRAME_CHARS)
                .rev()
                .find(|&i| label.is_char_boundary(i))
                .unwrap_or(0);
            label.truncate(cut);
            label.push('…');
        }
        out.push(label);
    }
    out
}

/// Frames that belong to the profiler or the allocation shims themselves.
/// Method symbols demangle with a leading `<` (e.g.
/// `<backtrace::...::Object>::parse`) and the alloc shims live in the
/// `__rustc::` namespace under the v0 mangling — strip/cover both. raw_vec
/// growth internals are dropped too (the Vec owner frame right above them
/// carries the attribution).
fn is_noise_frame(label: &str) -> bool {
    const NOISE_PREFIXES: &[&str] = &[
        "backtrace::",
        "std::backtrace",
        "__rustc::",
        "__rust_alloc",
        "__rust_realloc",
        "__rg_",
        "alloc::alloc::",
        "alloc::raw_vec::",
        "std::alloc::",
        "_Unwind_",
    ];
    let bare = label.strip_prefix('<').unwrap_or(label);
    label.contains("heap_profile") || NOISE_PREFIXES.iter().any(|prefix| bare.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crossings_are_size_weighted_and_unbiased() {
        let every = 64 * 1024 * 1024u64;
        // no crossing inside one interval
        assert_eq!(interval_crossings(0, every - 1, every), 0);
        // exact boundary crossing
        assert_eq!(interval_crossings(every - 1, 1, every), 1);
        // a huge allocation crosses ~size/every multiples in one call
        assert_eq!(interval_crossings(5, 10 * every, every), 10);
        // sum of crossings over any split of a byte stream equals the whole
        let (a, b, c) = (13 * 1024, 200 * 1024 * 1024, 7);
        let whole = interval_crossings(0, a + b + c, every);
        let split = interval_crossings(0, a, every)
            + interval_crossings(a, b, every)
            + interval_crossings(a + b, c, every);
        assert_eq!(whole, split);
    }

    #[test]
    fn filter_has_no_false_negatives() {
        for ptr in (0x7f31_0000_1000usize..).step_by(4096).take(1000) {
            filter_insert(ptr);
            assert!(filter_maybe_contains(ptr));
        }
    }

    #[test]
    fn reentrancy_guard_blocks_nested_entry() {
        let outer = ReentrancyGuard::enter();
        assert!(outer.is_some());
        assert!(ReentrancyGuard::enter().is_none());
        // regression (M27 probe): the BLOCKED attempt must not clear the
        // outer guard's flag — a second nested attempt must still block
        // (an eagerly-constructed-then-dropped guard once did clear it)
        assert!(ReentrancyGuard::enter().is_none());
        drop(outer);
        assert!(ReentrancyGuard::enter().is_some());
    }

    /// One combined lifecycle test (module state is process-global). The
    /// wrapper is NOT the test binary's global allocator, so enabling
    /// sampling here cannot affect other tests — only direct calls into the
    /// internals below observe it.
    #[test]
    fn sample_lifecycle_live_tracking_and_report() {
        // 1. off-state: the hot paths must not even account
        let flow_before = ALLOC_FLOW_BYTES.load(Ordering::Relaxed);
        on_alloc(0x1000 as *mut u8, 123_456);
        on_dealloc(0x1000 as *mut u8);
        assert_eq!(ALLOC_FLOW_BYTES.load(Ordering::Relaxed), flow_before);

        // 2. active: enable sampling for the direct-call checks below (the wrapper is not this test
        //    binary's global allocator, so nothing else observes this)
        SAMPLE_EVERY_BYTES.store(64 * 1024 * 1024, Ordering::Relaxed);

        // fake, never-dereferenced pointer keys, sampled through ONE call
        // site so all three collapse into one distinct stack
        let (p1, p2, p3) = (
            0x7f42_0000_0040usize,
            0x7f42_0000_2080usize,
            0x7f42_0000_4100usize,
        );
        let every = 64 * 1024 * 1024u64;
        let batch = [
            (p1, 1024u64, every),
            (p2, 2048, every),
            (p3, 10 * every, 10 * every), // huge alloc: 10 crossings
        ];
        for (ptr, size, weight) in batch {
            sample_allocation(ptr, size, weight);
        }
        assert!(filter_maybe_contains(p1) && filter_maybe_contains(p2));

        let lines = render_report(1);
        assert!(lines[0].contains("samples_live=3"), "header: {}", lines[0]);
        // p1+p2 share this test fn's call stack => one stack with 2 samples
        // and 128MB estimate; p3's stack differs only by size, same site —
        // all three collapse into ONE stack (same frames), 3 live samples
        let body = lines.join("\n");
        assert!(
            body.contains("count=3"),
            "expected one stack with 3 live samples: {body}"
        );
        assert!(
            body.contains("live_est=768.0MB"),
            "2x64MB + 640MB collapsed: {body}"
        );
        assert!(body.contains("rank=1"), "{body}");
        assert!(body.contains("stack=<"), "{body}");

        // frees remove live records and their contribution
        unsample_allocation(p3);
        unsample_allocation(0x7f42_dead_beefusize); // never sampled: no-op
        let lines = render_report(2);
        assert!(lines[0].contains("samples_live=2"), "header: {}", lines[0]);
        assert!(lines.join("\n").contains("live_est=128.0MB"), "{lines:?}");

        unsample_allocation(p1);
        unsample_allocation(p2);
        let lines = render_report(3);
        assert!(lines[0].contains("samples_live=0"), "header: {}", lines[0]);

        // a resampled recycled address must not double-count
        sample_allocation(p1, 4096, every);
        sample_allocation(p1, 4096, every); // teardown-skipped free, reused
        let lines = render_report(4);
        assert!(lines[0].contains("samples_live=1"), "header: {}", lines[0]);
        unsample_allocation(p1);

        SAMPLE_EVERY_BYTES.store(0, Ordering::Relaxed);
    }
}
