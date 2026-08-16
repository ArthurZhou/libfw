//! Task state machine and user control flags.
//!
//! The transfer state machine is `Idle → Downloading/Uploading → Paused →
//! Resumed → Completed/Failed`. Pause/resume/cancel are implemented as
//! `Cell`-backed flags checked cooperatively between chunks, which is safe
//! because WASM is single-threaded and each flag read is a cheap copy (no
//! borrows span `.await` points).

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use wasm_bindgen::JsValue;

use crate::error::LibfwError;
use crate::js::Callbacks;

/// Lifecycle state of the current transfer task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    /// No transfer is running.
    Idle,
    /// Downloading files from the server.
    Downloading,
    /// Uploading files to the server.
    Uploading,
    /// The active transfer is paused (user requested).
    Paused,
    /// The transfer finished successfully.
    Completed,
    /// The transfer failed (or was cancelled).
    Failed,
}

impl TaskState {
    /// Stable lowercase name exposed to JS.
    pub fn as_str(self) -> &'static str {
        match self {
            TaskState::Idle => "idle",
            TaskState::Downloading => "downloading",
            TaskState::Uploading => "uploading",
            TaskState::Paused => "paused",
            TaskState::Completed => "completed",
            TaskState::Failed => "failed",
        }
    }
}

/// A bounded async semaphore (single-threaded WASM friendly).
///
/// Grants at most `max` outstanding permits. The engine uses one shared
/// pool (sized by `concurrency`) so `concurrency` bounds the TOTAL number of
/// in-flight HTTP transfers — regardless of how many files or per-file
/// windows are active — which is what actually controls network parallelism.
#[derive(Debug, Clone)]
pub struct Semaphore {
    inner: Rc<SemaphoreInner>,
}

#[derive(Debug)]
struct SemaphoreInner {
    max: Cell<usize>,
    available: Cell<usize>,
    waiters: RefCell<VecDeque<Waker>>,
}

impl Semaphore {
    /// Create a pool with `max` permits (clamped to at least 1).
    pub fn new(max: usize) -> Self {
        let max = max.max(1);
        Semaphore {
            inner: Rc::new(SemaphoreInner {
                max: Cell::new(max),
                available: Cell::new(max),
                waiters: RefCell::new(VecDeque::new()),
            }),
        }
    }

    /// Reset to a full pool (called at the start of each transfer).
    pub fn reset(&self) {
        self.inner.available.set(self.inner.max.get());
        self.inner.waiters.borrow_mut().clear();
    }

    /// Resize the pool at runtime (adaptive tuning).
    ///
    /// Growing adds permits immediately; shrinking clamps outstanding
    /// availability so `available` never exceeds the new max (in-flight
    /// permits already granted are unaffected and release back into the
    /// smaller pool). Wakes waiters so a grow can unblock queued acquirers.
    pub fn set_max(&self, max: usize) {
        let max = max.max(1);
        let inner = &self.inner;
        let old = inner.max.get();
        let avail = inner.available.get();
        if max > old {
            inner.available.set((avail + (max - old)).min(max));
        } else {
            inner.available.set(avail.min(max));
        }
        inner.max.set(max);
        let waiters = std::mem::take(&mut *inner.waiters.borrow_mut());
        for w in waiters {
            w.wake();
        }
    }

    /// Acquire one permit, waiting asynchronously when the pool is empty.
    pub fn acquire(&self) -> Acquire<'_> {
        Acquire { sem: self }
    }

    fn try_acquire(&self) -> bool {
        let avail = self.inner.available.get();
        if avail > 0 {
            self.inner.available.set(avail - 1);
            true
        } else {
            false
        }
    }

    fn release(&self) {
        let avail = self.inner.available.get();
        self.inner.available.set((avail + 1).min(self.inner.max.get()));
        // Wake every waiter; each re-poll re-checks the pool and either
        // acquires (removed from the queue) or re-queues itself.
        let waiters = std::mem::take(&mut *self.inner.waiters.borrow_mut());
        for w in waiters {
            w.wake();
        }
    }
}

/// Future returned by [`Semaphore::acquire`].
pub struct Acquire<'a> {
    sem: &'a Semaphore,
}

impl Future for Acquire<'_> {
    type Output = Permit;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Permit> {
        if self.sem.try_acquire() {
            return Poll::Ready(Permit {
                sem: self.sem.clone(),
            });
        }
        self.sem.inner.waiters.borrow_mut().push_back(cx.waker().clone());
        Poll::Pending
    }
}

/// RAII permit; the held slot is returned to the pool on drop.
pub struct Permit {
    sem: Semaphore,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.sem.release();
    }
}

/// Shared, mutable task control state.
///
/// Every field is `Rc<Cell<_>>` so that **clones share the same state**.
/// The WASM facade hands an owned clone to each transfer future, and the
/// transfer clones it again into per-file tasks; without `Rc` those clones
/// would *copy* the byte counters and state flags, so progress would report
/// `0` and `pause`/`resume`/`cancel` would never reach the running task.
/// `Rc` is sound here because WASM is single-threaded.
#[derive(Debug, Clone)]
pub struct TaskControl {
    state: Rc<Cell<TaskState>>,
    /// The state to restore on `resume()` (downloading/uploading).
    active: Rc<Cell<TaskState>>,
    cancelled: Rc<Cell<bool>>,
    done_bytes: Rc<Cell<u64>>,
    total_bytes: Rc<Cell<u64>>,
    /// `done_bytes` the last time progress was reported to JS, used to
    /// throttle `on_progress` events (see [`TaskControl::report_progress_if`]).
    last_reported: Rc<Cell<u64>>,
    /// Global cap on in-flight HTTP transfers (see [`Semaphore`]).
    semaphore: Semaphore,
}

impl Default for TaskControl {
    fn default() -> Self {
        TaskControl::new()
    }
}

impl TaskControl {
    /// Create a fresh, idle control block (default global parallelism).
    pub fn new() -> Self {
        TaskControl::with_max_parallel(libfw_core::DEFAULT_CONCURRENCY)
    }

    /// Create a fresh control block whose global in-flight HTTP transfer pool
    /// is capped at `max_parallel` (from the client's `concurrency` option).
    pub fn with_max_parallel(max_parallel: usize) -> Self {
        TaskControl {
            state: Rc::new(Cell::new(TaskState::Idle)),
            active: Rc::new(Cell::new(TaskState::Idle)),
            cancelled: Rc::new(Cell::new(false)),
            done_bytes: Rc::new(Cell::new(0)),
            total_bytes: Rc::new(Cell::new(0)),
            last_reported: Rc::new(Cell::new(0)),
            semaphore: Semaphore::new(max_parallel),
        }
    }

    /// Reset everything for a new transfer (refills the permit pool).
    pub fn reset(&self) {
        self.state.set(TaskState::Idle);
        self.active.set(TaskState::Idle);
        self.cancelled.set(false);
        self.done_bytes.set(0);
        self.total_bytes.set(0);
        self.last_reported.set(0);
        self.semaphore.reset();
    }

    /// The global in-flight transfer pool (shared by every file/chunk task).
    pub fn semaphore(&self) -> &Semaphore {
        &self.semaphore
    }

    /// Resize the global pool at runtime (adaptive tuning changes the
    /// concurrency dimension mid-transfer).
    pub fn set_max_parallel(&self, max_parallel: usize) {
        self.semaphore.set_max(max_parallel);
    }

    /// Current state.
    pub fn state(&self) -> TaskState {
        self.state.get()
    }

    /// Transition into an active state (`Downloading`/`Uploading`).
    pub fn begin(&self, s: TaskState) {
        self.active.set(s);
        self.state.set(s);
    }

    /// Mark the transfer completed.
    pub fn complete(&self) {
        self.state.set(TaskState::Completed);
    }

    /// Mark the transfer failed.
    pub fn fail(&self) {
        self.state.set(TaskState::Failed);
    }

    /// Pause: remember the active state, then go `Paused`.
    pub fn pause(&self) {
        if matches!(
            self.state.get(),
            TaskState::Downloading | TaskState::Uploading
        ) {
            self.active.set(self.state.get());
            self.state.set(TaskState::Paused);
        }
    }

    /// Resume: restore the remembered active state.
    pub fn resume(&self) {
        if self.state.get() == TaskState::Paused {
            self.state.set(self.active.get());
        }
    }

    /// Request cancellation. Cooperating loops observe it via
    /// [`TaskControl::check`] / [`TaskControl::wait_ready`].
    pub fn cancel(&self) {
        self.cancelled.set(true);
        if !matches!(self.state.get(), TaskState::Completed | TaskState::Failed) {
            self.state.set(TaskState::Failed);
        }
    }

    /// Whether cancellation was requested.
    #[allow(dead_code)] // public control API; used by tests and the SDK
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.get()
    }

    /// Throw [`LibfwError::Cancelled`] when the user cancelled.
    pub fn check(&self) -> Result<(), LibfwError> {
        if self.cancelled.get() {
            Err(LibfwError::Cancelled)
        } else {
            Ok(())
        }
    }

    /// Block until the task is neither paused nor cancelled, yielding to
    /// the JS event loop so `pause`/`resume`/`cancel` can be delivered.
    pub async fn wait_ready(&self) -> Result<(), LibfwError> {
        loop {
            self.check()?;
            if self.state.get() != TaskState::Paused {
                return Ok(());
            }
            yield_to_event_loop().await;
        }
    }

    /// Record progress made.
    pub fn add_progress(&self, bytes: u64) {
        self.done_bytes.set(self.done_bytes.get().saturating_add(bytes));
    }

    /// Remove previously-counted progress (saturating).
    ///
    /// Exposed as a helper for callers (e.g. the SDK) that need to undo
    /// progress they counted but later decided not to transfer. Not used
    /// internally by the current engine, hence `allow(dead_code)`.
    #[allow(dead_code)]
    pub fn subtract_progress(&self, bytes: u64) {
        self.done_bytes.set(self.done_bytes.get().saturating_sub(bytes));
    }

    /// Bytes transferred so far.
    pub fn done_bytes(&self) -> u64 {
        self.done_bytes.get()
    }

    /// Total bytes to transfer (may be 0 until known).
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.get()
    }

    /// Set the expected total (e.g. sum of file sizes).
    pub fn set_total(&self, bytes: u64) {
        self.total_bytes.set(bytes);
    }

    /// Progress in `[0.0, 1.0]` (0 when the total is unknown).
    pub fn progress(&self) -> f64 {
        let total = self.total_bytes.get();
        if total == 0 {
            0.0
        } else {
            (self.done_bytes.get() as f64 / total as f64).clamp(0.0, 1.0)
        }
    }

    /// Emit an `on_progress` event to JS whenever the transferred byte count
    /// changes (i.e. at block granularity), so uploads and downloads report
    /// progress in real time instead of stalling on whole-percent buckets.
    ///
    /// Reporting per change keeps the bar moving continuously for large
    /// files: a single block (e.g. 2 MiB) is often far less than 1% of a big
    /// file, so bucketing to whole-percents made progress appear frozen in
    /// coarse jumps. The event is cheap (one sync JS call), so firing once
    /// per block is acceptable; callers that already report after each
    /// file's completion remain correct.
    ///
    /// Uses the shared [`TaskControl`] as the single source of truth, so
    /// concurrent per-file tasks all feed one coherent progress bar.
    ///
    /// Returns `Ok(())` when nothing was emitted (`done` unchanged) or when
    /// the (cheap sync) event was delivered.
    pub fn report_progress_if(&self, callbacks: &Callbacks) -> Result<(), LibfwError> {
        let total = self.total_bytes.get();
        let done = self.done_bytes.get();
        if total == 0 {
            // Total unknown: still push a best-effort bytes-only event so
            // consumers see *something* (e.g. single-file downloads before
            // the server reports a size).
            let last = self.last_reported.get();
            if done != last {
                self.last_reported.set(done);
                return callbacks.on_progress(done, 0);
            }
            return Ok(());
        }
        // Clamp so a rare gap-fill re-send (which re-counts a few bytes) can
        // never push the reported fraction past 100%.
        let reported = done.min(total);
        let last = self.last_reported.get().min(total);
        if reported != last {
            self.last_reported.set(reported);
            return callbacks.on_progress(reported, total);
        }
        Ok(())
    }
}

/// Yield control back to the JS event loop by awaiting a resolved promise.
///
/// Only ever awaited from the WASM transfer loops; native unit tests never
/// reach this path.
async fn yield_to_event_loop() {
    let promise = js_sys::Promise::resolve(&JsValue::UNDEFINED);
    let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::pin;
    use std::task::{Context, RawWaker, RawWakerVTable, Waker};

    /// A waker that does nothing (manual-poll tests only).
    fn noop_waker() -> Waker {
        fn noop(_: *const ()) {}
        fn clone(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        static VTABLE: RawWakerVTable =
            RawWakerVTable::new(clone, noop, noop, noop);
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    #[test]
    fn semaphore_blocks_when_exhausted_and_recovers() {
        let sem = Semaphore::new(2);
        let p1 = futures::executor::block_on(sem.acquire());
        let p2 = futures::executor::block_on(sem.acquire());

        // Pool exhausted → a third acquire must stay Pending.
        let mut fut = pin!(sem.acquire());
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(fut.as_mut().poll(&mut cx).is_pending());

        // Freeing one permit lets the waiter proceed.
        drop(p1);
        assert!(fut.as_mut().poll(&mut cx).is_ready());
        drop(p2);

        // reset() restores the full pool.
        sem.reset();
        let _ = futures::executor::block_on(sem.acquire());
        let _ = futures::executor::block_on(sem.acquire());
    }

    #[test]
    fn semaphore_clones_share_the_pool() {
        let a = Semaphore::new(1);
        let b = a.clone();
        let p = futures::executor::block_on(a.acquire());
        // A clone of the pool sees the same exhaustion.
        let mut fut = pin!(b.acquire());
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(fut.as_mut().poll(&mut cx).is_pending());
        drop(p);
        assert!(fut.as_mut().poll(&mut cx).is_ready());
    }

    #[test]
    fn idle_state_machine() {
        let c = TaskControl::new();
        assert_eq!(c.state(), TaskState::Idle);
        c.begin(TaskState::Downloading);
        assert_eq!(c.state(), TaskState::Downloading);
        c.pause();
        assert_eq!(c.state(), TaskState::Paused);
        c.resume();
        assert_eq!(c.state(), TaskState::Downloading);
        c.complete();
        assert_eq!(c.state(), TaskState::Completed);
    }

    #[test]
    fn pause_restores_active_state() {
        let c = TaskControl::new();
        c.begin(TaskState::Uploading);
        c.pause();
        assert_eq!(c.state(), TaskState::Paused);
        c.resume();
        assert_eq!(c.state(), TaskState::Uploading);
    }

    #[test]
    fn cancel_marks_failed_and_checks() {
        let c = TaskControl::new();
        c.begin(TaskState::Downloading);
        c.cancel();
        assert!(c.is_cancelled());
        assert!(matches!(c.check(), Err(LibfwError::Cancelled)));
        assert_eq!(c.state(), TaskState::Failed);
    }

    #[test]
    fn progress_is_bounded() {
        let c = TaskControl::new();
        c.set_total(100);
        c.add_progress(25);
        assert_eq!(c.progress(), 0.25);
        c.add_progress(200);
        assert_eq!(c.progress(), 1.0);
    }

    #[test]
    fn clones_share_counters_and_flags() {
        // Regression: the WASM facade clones the control into each transfer
        // task. Clones must share progress and pause/cancel state, otherwise
        // `done_bytes()`/`progress()` report 0 and controls never reach the
        // running task.
        let c = TaskControl::new();
        let task = c.clone();
        c.set_total(100);
        task.add_progress(40);
        assert_eq!(c.done_bytes(), 40);
        assert_eq!(c.progress(), 0.4);
        // Controls issued on one handle must be visible to the task's handle.
        c.begin(TaskState::Downloading);
        c.pause();
        assert_eq!(task.state(), TaskState::Paused);
        c.resume();
        assert_eq!(task.state(), TaskState::Downloading);
        c.cancel();
        assert!(matches!(task.check(), Err(LibfwError::Cancelled)));
        assert!(task.is_cancelled());
    }

    #[test]
    fn state_names_are_stable() {
        assert_eq!(TaskState::Idle.as_str(), "idle");
        assert_eq!(TaskState::Paused.as_str(), "paused");
        assert_eq!(TaskState::Completed.as_str(), "completed");
        assert_eq!(TaskState::Failed.as_str(), "failed");
    }
}
