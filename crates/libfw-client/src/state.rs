//! Task state machine and user control flags.
//!
//! The transfer state machine is `Idle → Downloading/Uploading → Paused →
//! Resumed → Completed/Failed`. Pause/resume/cancel are implemented as
//! `Cell`-backed flags checked cooperatively between chunks, which is safe
//! because WASM is single-threaded and each flag read is a cheap copy (no
//! borrows span `.await` points).

use std::cell::Cell;
use std::rc::Rc;

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
}

impl Default for TaskControl {
    fn default() -> Self {
        TaskControl::new()
    }
}

impl TaskControl {
    /// Create a fresh, idle control block.
    pub fn new() -> Self {
        TaskControl {
            state: Rc::new(Cell::new(TaskState::Idle)),
            active: Rc::new(Cell::new(TaskState::Idle)),
            cancelled: Rc::new(Cell::new(false)),
            done_bytes: Rc::new(Cell::new(0)),
            total_bytes: Rc::new(Cell::new(0)),
            last_reported: Rc::new(Cell::new(0)),
        }
    }

    /// Reset everything for a new transfer.
    pub fn reset(&self) {
        self.state.set(TaskState::Idle);
        self.active.set(TaskState::Idle);
        self.cancelled.set(false);
        self.done_bytes.set(0);
        self.total_bytes.set(0);
        self.last_reported.set(0);
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

    /// Emit an `on_progress` event to JS, throttled to whole-percent
    /// boundaries of `total` so a long single-file transfer reports smooth
    /// intermediate values instead of only 0% then 100%.
    ///
    /// Uses the shared [`TaskControl`] as the single source of truth, so
    /// concurrent per-file tasks all feed one coherent progress bar. The
    /// final `done == total` boundary is always reported; callers that
    /// already report after each file's completion remain correct.
    ///
    /// Returns `Ok(())` when nothing was emitted (still within the current
    /// percent bucket) or when the (cheap sync) event was delivered.
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
        let last = self.last_reported.get();
        let done_pct = done.saturating_mul(100) / total;
        let last_pct = last.saturating_mul(100) / total;
        if done_pct != last_pct || done >= total {
            self.last_reported.set(done);
            return callbacks.on_progress(done, total);
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
