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

//! Process-wide split runtime for Vortex writers.
//!
//! Controller futures and blocking I/O stay off the CPU pool. Only closures
//! explicitly submitted through Vortex's `spawn_cpu` contract enter the fixed,
//! bounded worker pool, so concurrent writers borrow idle workers instead of
//! constructing private per-merge Tokio runtimes.

use std::{
    cell::Cell,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{SyncSender, sync_channel},
    },
};

use futures::future::BoxFuture;
use vortex::io::runtime::{AbortHandle, AbortHandleRef, Executor, Handle};

use crate::VixError;

static CONFIGURED_THREADS: AtomicUsize = AtomicUsize::new(0);
static SHARED_CPU_RUNTIME: OnceLock<Result<SharedCpuRuntime, String>> = OnceLock::new();

thread_local! {
    static IN_CPU_LEAF: Cell<bool> = const { Cell::new(false) };
}

type CpuJob = Box<dyn FnOnce() + Send + 'static>;

struct SharedCpuRuntime {
    threads: usize,
    _executor: Arc<dyn Executor>,
    handle: Handle,
    _io_runtime: tokio::runtime::Runtime,
}

struct SplitExecutor {
    io: tokio::runtime::Handle,
    cpu: CpuLeafPool,
}

struct CpuLeafPool {
    sender: Option<SyncSender<CpuJob>>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl CpuLeafPool {
    fn new(threads: usize) -> Result<Self, String> {
        // Vortex writer streams already bound their per-writer outstanding
        // chunk count. This process bound prevents several writers from
        // accumulating unbounded captured chunks at once.
        let queue_capacity = threads.saturating_mul(4).max(1);
        let (sender, receiver) = sync_channel::<CpuJob>(queue_capacity);
        let receiver = Arc::new(std::sync::Mutex::new(receiver));
        let mut workers = Vec::with_capacity(threads);
        for index in 0..threads {
            let receiver = Arc::clone(&receiver);
            let worker = std::thread::Builder::new()
                .name(format!("vix-cpu-{index}"))
                .spawn(move || {
                    loop {
                        let job = match receiver.lock() {
                            Ok(receiver) => receiver.recv(),
                            Err(_) => return,
                        };
                        let Ok(job) = job else { return };
                        IN_CPU_LEAF.with(|inside| {
                            debug_assert!(!inside.replace(true));
                            let _ = catch_unwind(AssertUnwindSafe(job));
                            inside.set(false);
                        });
                    }
                })
                .map_err(|error| format!("spawn VIX CPU worker {index}: {error}"))?;
            workers.push(worker);
        }
        Ok(Self {
            sender: Some(sender),
            workers,
        })
    }

    fn submit(&self, job: CpuJob) {
        IN_CPU_LEAF.with(|inside| {
            assert!(
                !inside.get(),
                "a VIX CPU leaf attempted to submit child work to the same bounded pool"
            );
        });
        self.sender
            .as_ref()
            .expect("VIX CPU pool sender is live")
            .send(job)
            .expect("VIX CPU workers remain live for the process lifetime");
    }
}

impl Drop for CpuLeafPool {
    fn drop(&mut self) {
        self.sender.take();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

struct CpuAbortHandle(Arc<AtomicBool>);

impl AbortHandle for CpuAbortHandle {
    fn abort(self: Box<Self>) {
        self.0.store(true, Ordering::Release);
    }
}

impl Executor for SplitExecutor {
    fn spawn(&self, future: BoxFuture<'static, ()>) -> AbortHandleRef {
        Box::new(self.io.spawn(future).abort_handle())
    }

    fn spawn_io(&self, future: BoxFuture<'static, ()>) -> AbortHandleRef {
        Box::new(self.io.spawn(future).abort_handle())
    }

    fn spawn_cpu(&self, task: CpuJob) -> AbortHandleRef {
        let cancelled = Arc::new(AtomicBool::new(false));
        let task_cancelled = Arc::clone(&cancelled);
        self.cpu.submit(Box::new(move || {
            if !task_cancelled.load(Ordering::Acquire) {
                task();
            }
        }));
        Box::new(CpuAbortHandle(cancelled))
    }

    fn spawn_blocking_io(&self, task: CpuJob) -> AbortHandleRef {
        Box::new(self.io.spawn_blocking(task).abort_handle())
    }
}

/// Configure the role-sized process CPU pool before the first parallel writer
/// starts. Multiple producers may call this; the largest pre-initialization
/// request wins. The pool is immutable after construction.
pub fn configure_shared_cpu_executor(threads: usize) {
    let requested = threads.max(1);
    CONFIGURED_THREADS.fetch_max(requested, Ordering::Relaxed);
    if let Some(Ok(runtime)) = SHARED_CPU_RUNTIME.get()
        && runtime.threads != requested
    {
        log::debug!(
            "vix shared CPU executor already has {} workers; ignoring later request for {requested}",
            runtime.threads
        );
    }
}

fn configured_threads() -> usize {
    let configured = CONFIGURED_THREADS.load(Ordering::Relaxed);
    if configured > 0 {
        configured
    } else {
        std::thread::available_parallelism()
            .map_or(1, |parallelism| parallelism.get().saturating_sub(2).max(1))
    }
}

fn build_runtime() -> Result<SharedCpuRuntime, String> {
    let threads = configured_threads();
    let io_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("vix-io")
        .enable_all()
        .build()
        .map_err(|error| format!("shared VIX I/O executor: {error}"))?;
    let concrete = Arc::new(SplitExecutor {
        io: io_runtime.handle().clone(),
        cpu: CpuLeafPool::new(threads)?,
    });
    let executor: Arc<dyn Executor> = concrete;
    let handle = Handle::new(Arc::downgrade(&executor));
    Ok(SharedCpuRuntime {
        threads,
        _executor: executor,
        handle,
        _io_runtime: io_runtime,
    })
}

/// Session handle whose orchestration/I/O and CPU work use disjoint executors.
pub fn shared_vortex_execution_handle() -> Result<Handle, VixError> {
    match SHARED_CPU_RUNTIME.get_or_init(build_runtime) {
        Ok(runtime) => Ok(runtime.handle.clone()),
        Err(error) => Err(VixError::Writer(error.clone())),
    }
}

#[cfg(test)]
pub(crate) fn shared_cpu_thread_count() -> Result<usize, VixError> {
    shared_vortex_execution_handle()?;
    match SHARED_CPU_RUNTIME.get().expect("runtime initialized") {
        Ok(runtime) => Ok(runtime.threads),
        Err(error) => Err(VixError::Writer(error.clone())),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use vortex::io::runtime::{BlockingRuntime, single::SingleThreadRuntime};

    use super::*;

    #[test]
    fn shared_executor_bounds_cpu_and_separates_io() {
        configure_shared_cpu_executor(2);
        let threads = shared_cpu_thread_count().unwrap();
        let handle = shared_vortex_execution_handle().unwrap();
        let active = Arc::new(AtomicUsize::new(0));
        let high_water = Arc::new(AtomicUsize::new(0));
        let tasks: Vec<_> = (0..threads.saturating_mul(2))
            .map(|_| {
                let active = Arc::clone(&active);
                let high_water = Arc::clone(&high_water);
                handle.spawn_cpu(move || {
                    let now = active.fetch_add(1, Ordering::AcqRel) + 1;
                    high_water.fetch_max(now, Ordering::AcqRel);
                    let name = std::thread::current()
                        .name()
                        .unwrap_or_default()
                        .to_string();
                    std::thread::sleep(Duration::from_millis(5));
                    active.fetch_sub(1, Ordering::AcqRel);
                    name
                })
            })
            .collect();
        let driver = SingleThreadRuntime::default();
        let names = driver.block_on(futures::future::join_all(tasks));
        assert!(high_water.load(Ordering::Acquire) <= threads);
        assert!(names.iter().all(|name| name.starts_with("vix-cpu-")));

        let io_name = driver.block_on(handle.spawn(async {
            std::thread::current()
                .name()
                .unwrap_or_default()
                .to_string()
        }));
        assert!(io_name.starts_with("vix-io"));
    }

    #[test]
    fn cpu_leaf_cannot_submit_nested_cpu_work() {
        let handle = shared_vortex_execution_handle().unwrap();
        let nested = handle.clone();
        let task = handle.spawn_cpu(move || {
            let _child = nested.spawn_cpu(|| 1usize);
        });
        let driver = SingleThreadRuntime::default();
        let panic = catch_unwind(AssertUnwindSafe(|| driver.block_on(task)));
        assert!(panic.is_err(), "nested CPU submission must fail loudly");
    }

    #[test]
    fn leaf_queue_backpressures_only_the_submitter() {
        let pool = Arc::new(CpuLeafPool::new(1).unwrap());
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        pool.submit(Box::new(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        }));
        started_rx.recv().unwrap();
        for _ in 0..4 {
            pool.submit(Box::new(|| {}));
        }

        let (submitted_tx, submitted_rx) = std::sync::mpsc::channel();
        let submit_pool = Arc::clone(&pool);
        let submitter = std::thread::spawn(move || {
            submit_pool.submit(Box::new(|| {}));
            submitted_tx.send(()).unwrap();
        });
        assert!(
            submitted_rx
                .recv_timeout(Duration::from_millis(20))
                .is_err(),
            "a full queue must backpressure its non-leaf submitter"
        );
        release_tx.send(()).unwrap();
        submitted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        submitter.join().unwrap();
        drop(pool);
    }
}
