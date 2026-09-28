//! Event-driven wake-ups for the background reconcile loop.
//!
//! Commit points that can change what a reconcile pass would do (desired-state commits, observed
//! state merges, provider/executor state published over local IPC, maintenance changes, new
//! integrations) call [`NodeApp::request_reconcile`]. The loop sleeps until such a request, or
//! until the periodic backstop elapses, instead of polling on every reconcile interval.

use super::{NodeApp, NodeError, ReconcileLoopHandle};
use crate::config::normalize_runtime_tuning_duration;
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::{
    sync::{Notify, watch},
    time::Instant,
};
use tracing::error;

/// Upper bound on how long a wake-up waits so a burst of updates collapses into one pass.
const RECONCILE_WAKE_DEBOUNCE: Duration = Duration::from_millis(5);

/// Reconcile request bookkeeping shared by every clone of a [`NodeApp`].
///
/// `requested` counts wake-up requests. Each reconcile pass records the count it observed when
/// it started in `served`, so the loop can skip requests a synchronous `tick()` already covered.
#[derive(Default)]
pub(super) struct ReconcileTrigger {
    requested: AtomicU64,
    served: AtomicU64,
    attached_loops: AtomicUsize,
    notify: Notify,
}

impl ReconcileTrigger {
    fn request(&self) {
        self.requested.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_one();
    }

    /// Marks every request made so far as covered by the pass that is starting now. The pass
    /// reads state after this point, so it observes every mutation committed before them.
    pub(super) fn begin_pass(&self) {
        let requested = self.requested.load(Ordering::SeqCst);
        self.served.fetch_max(requested, Ordering::SeqCst);
    }

    fn pending(&self) -> bool {
        self.requested.load(Ordering::SeqCst) > self.served.load(Ordering::SeqCst)
    }

    fn loop_attached(&self) -> bool {
        self.attached_loops.load(Ordering::SeqCst) > 0
    }
}

/// Keeps the trigger marked as serviced by a background loop until the loop task exits.
struct AttachedLoop(Arc<super::state_access::NodeState>);

impl Drop for AttachedLoop {
    fn drop(&mut self) {
        self.0
            .reconcile
            .attached_loops
            .fetch_sub(1, Ordering::SeqCst);
    }
}

impl NodeApp {
    /// Asks the background reconcile loop to run a pass soon.
    ///
    /// Orion calls this at its own commit points. Embedders with in-process provider or executor
    /// integrations whose snapshots change outside Orion's control can call it to have the change
    /// reconciled before the next backstop pass. Without a running loop this is a no-op beyond
    /// bookkeeping.
    pub fn request_reconcile(&self) {
        self.state.reconcile.request();
    }

    /// Reconciles after a committed local change: defers to the background loop when one is
    /// running (bursts coalesce there), otherwise reconciles synchronously as before.
    pub(super) fn reconcile_after_change(&self) -> Result<(), NodeError> {
        self.request_reconcile();
        if self.state.reconcile.loop_attached() {
            return Ok(());
        }
        self.tick().map(|_| ())
    }

    pub(super) async fn reconcile_after_change_async(&self) -> Result<(), NodeError> {
        self.request_reconcile();
        if self.state.reconcile.loop_attached() {
            return Ok(());
        }
        self.tick_async().await.map(|_| ())
    }

    /// Spawns the event-driven reconcile loop.
    ///
    /// `interval` is the minimum idle gap between the end of one pass and the start of the next
    /// (`ORION_NODE_RECONCILE_MS`). The loop runs a pass at startup, then waits for a
    /// [`NodeApp::request_reconcile`] wake-up (coalesced for up to a few milliseconds and never
    /// sooner than `interval` after the previous pass) or for the backstop
    /// (`NodeRuntimeTuning::reconcile_backstop_interval`, clamped to at least `interval`).
    pub fn spawn_reconcile_loop(&self, interval: Duration) -> ReconcileLoopHandle {
        let app = self.clone();
        let spacing = normalize_runtime_tuning_duration(interval);
        let backstop = self
            .config
            .runtime_tuning
            .reconcile_backstop_interval
            .max(spacing);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        // Attach before spawning so post-mutation reconciles defer to the loop immediately.
        self.state
            .reconcile
            .attached_loops
            .fetch_add(1, Ordering::SeqCst);
        let attached = AttachedLoop(self.state.clone());

        let task = tokio::spawn(async move {
            let _attached = attached;
            loop {
                if *shutdown_rx.borrow() {
                    break;
                }
                if let Err(err) = app.tick_async().await {
                    error!(node = %app.config.node_id, task = "reconcile", error = %err, "background loop iteration failed");
                    // Retry after the normal spacing rather than waiting for the backstop.
                    app.request_reconcile();
                }
                let wait = wait_for_next_pass(
                    &app.state.reconcile,
                    &mut shutdown_rx,
                    Instant::now(),
                    spacing,
                    backstop,
                );
                if !wait.await {
                    break;
                }
            }
        });

        ReconcileLoopHandle {
            shutdown_tx,
            task: Arc::new(std::sync::Mutex::new(Some(task))),
        }
    }
}

/// Waits until the next reconcile pass should start. Returns `false` when the loop should stop
/// (shutdown requested or the handle was dropped).
async fn wait_for_next_pass(
    trigger: &ReconcileTrigger,
    shutdown_rx: &mut watch::Receiver<bool>,
    last_pass: Instant,
    spacing: Duration,
    backstop: Duration,
) -> bool {
    let backstop_at = last_pass + backstop;
    loop {
        if trigger.pending() {
            break;
        }
        tokio::select! {
            changed = shutdown_rx.changed() => {
                // A dropped handle closes the channel; stop instead of spinning on the
                // immediately-ready `changed()` error.
                if changed.is_err() || *shutdown_rx.borrow() {
                    return false;
                }
            }
            // `Notify` stores a permit when nobody is waiting, so a request made while a pass
            // was running still wakes this wait; `pending()` then filters requests the pass or a
            // synchronous tick already served.
            _ = trigger.notify.notified() => {}
            _ = tokio::time::sleep_until(backstop_at) => return true,
        }
    }

    let run_at = (Instant::now() + RECONCILE_WAKE_DEBOUNCE.min(spacing)).max(last_pass + spacing);
    tokio::select! {
        changed = shutdown_rx.changed() => changed.is_ok() && !*shutdown_rx.borrow(),
        _ = tokio::time::sleep_until(run_at) => true,
    }
}
