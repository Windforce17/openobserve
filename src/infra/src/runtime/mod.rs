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

use std::sync::{Arc, LazyLock as Lazy};

use config::get_config;
use tokio::runtime::Runtime;

const THREAD_STACK_SIZE: usize = 16 * 1024 * 1024;

fn configure_runtime(builder: tokio::runtime::Builder) -> tokio::runtime::Builder {
    let mut builder = config::allocator_reclaim::configure_runtime(builder);
    if !crate::diagnostics::fatal_signal_enabled() {
        return builder;
    }
    // This runs for both the caller driving block_on and Tokio's worker/blocking
    // threads. The allocator's on_thread_park hook remains untouched.
    if let Err(error) = crate::diagnostics::prepare_current_thread() {
        eprintln!("fatal diagnostics alternate stack unavailable: {error}");
    }
    builder.on_thread_start(|| {
        if let Err(error) = crate::diagnostics::prepare_current_thread() {
            eprintln!("fatal diagnostics alternate stack unavailable: {error}");
        }
    });
    builder
}

pub fn create_job_runtime() -> std::io::Result<Runtime> {
    let cfg = get_config();
    configure_runtime(tokio::runtime::Builder::new_multi_thread())
        .thread_name("job_runtime")
        .thread_stack_size(THREAD_STACK_SIZE)
        .worker_threads(cfg.limit.job_runtime_worker_num)
        .max_blocking_threads(cfg.limit.job_runtime_blocking_worker_num)
        .enable_all()
        .build()
}

// The HTTP leader parses and rewrites SQL on this runtime, so it needs the
// same deep-recursion headroom as the other runtimes (a long OR/IN chain
// otherwise overflows the tokio default 2 MiB worker stack and aborts the
// process — #14469).
pub fn create_main_runtime() -> std::io::Result<Runtime> {
    configure_runtime(tokio::runtime::Builder::new_multi_thread())
        .thread_name("main_runtime")
        .thread_stack_size(THREAD_STACK_SIZE)
        .enable_all()
        .build()
}

pub fn create_grpc_runtime() -> std::io::Result<Runtime> {
    let cfg = get_config();
    configure_runtime(tokio::runtime::Builder::new_multi_thread())
        .thread_name("grpc_runtime")
        .thread_stack_size(THREAD_STACK_SIZE)
        .worker_threads(cfg.limit.grpc_runtime_worker_num)
        .max_blocking_threads(cfg.limit.grpc_runtime_blocking_worker_num)
        .enable_all()
        .build()
}

pub static DATAFUSION_RUNTIME: Lazy<Arc<Runtime>> = Lazy::new(|| {
    Arc::new(
        configure_runtime(tokio::runtime::Builder::new_multi_thread())
            .thread_name("datafusion_runtime")
            .thread_stack_size(THREAD_STACK_SIZE)
            .worker_threads(get_config().limit.cpu_num)
            .enable_all()
            .build()
            .unwrap(),
    )
});

pub static WAL_RUNTIME: Lazy<Option<Arc<Runtime>>> = Lazy::new(|| {
    let cfg = get_config();

    if !cfg.common.wal_dedicated_runtime_enabled {
        return None;
    }

    let total_cpus = cfg.limit.cpu_num;
    // Security Check: At least 2 CPU cores are required for isolation (1 for HTTP, 1 for WAL)
    if total_cpus < 2 {
        return None;
    }

    // Worker number strategy for WAL runtime
    let thread_num = if cfg.limit.wal_runtime_worker_num > 0 {
        cfg.limit.wal_runtime_worker_num
    } else {
        cfg.limit.mem_table_bucket_num
    };
    // Ensure the number of worker threads less than the total number of CPU cores
    let thread_num = thread_num.min(total_cpus);

    configure_runtime(tokio::runtime::Builder::new_multi_thread())
        .thread_name("wal-runtime")
        .thread_stack_size(THREAD_STACK_SIZE)
        .worker_threads(thread_num)
        .enable_all()
        .build()
        .ok()
        .map(Arc::new)
});

#[cfg(all(
    test,
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_stacks_cover_runtime_and_blocking_threads() {
        let Ok(enabled) = std::env::var("O2_RUNTIME_STACK_CHILD") else {
            for value in ["true", "false"] {
                let output = std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "runtime::tests::diagnostic_stacks_cover_runtime_and_blocking_threads",
                        "--nocapture",
                    ])
                    .env("O2_RUNTIME_STACK_CHILD", value)
                    .env("ZO_FATAL_SIGNAL_BACKTRACE", value)
                    .env_remove("ZO_COMPACT_MERGE_START_LOG")
                    .output()
                    .unwrap();
                assert!(output.status.success(), "{output:?}");
            }
            return;
        };
        crate::diagnostics::init_from_env().unwrap();
        let enabled = enabled == "true";
        fn stack_size() -> usize {
            unsafe {
                let mut stack: libc::stack_t = std::mem::zeroed();
                assert_eq!(libc::sigaltstack(std::ptr::null(), &mut stack), 0);
                stack.ss_size
            }
        }
        fn check(size: usize, enabled: bool) {
            if enabled {
                assert!(size >= 65536, "{size}");
            } else {
                assert!(size < 65536, "disabled changed Rust's stack: {size}");
            }
        }
        let runtime = configure_runtime(tokio::runtime::Builder::new_multi_thread())
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        check(stack_size(), enabled);
        runtime.block_on(async {
            check(tokio::spawn(async { stack_size() }).await.unwrap(), enabled);
            check(
                tokio::task::spawn_blocking(stack_size).await.unwrap(),
                enabled,
            );
        });
        drop(runtime);
    }
}
