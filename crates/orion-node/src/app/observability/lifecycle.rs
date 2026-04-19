use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Default)]
pub(crate) struct LifecycleState {
    replay_completed: AtomicBool,
    replay_successful: AtomicBool,
    http_bound: AtomicBool,
    ipc_bound: AtomicBool,
    ipc_stream_bound: AtomicBool,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct LifecycleSnapshot {
    pub(crate) replay_completed: bool,
    pub(crate) replay_successful: bool,
    pub(crate) http_bound: bool,
    pub(crate) ipc_bound: bool,
    pub(crate) ipc_stream_bound: bool,
}

impl LifecycleState {
    pub(crate) fn snapshot(&self) -> LifecycleSnapshot {
        LifecycleSnapshot {
            replay_completed: self.replay_completed.load(Ordering::SeqCst),
            replay_successful: self.replay_successful.load(Ordering::SeqCst),
            http_bound: self.http_bound.load(Ordering::SeqCst),
            ipc_bound: self.ipc_bound.load(Ordering::SeqCst),
            ipc_stream_bound: self.ipc_stream_bound.load(Ordering::SeqCst),
        }
    }

    pub(crate) fn set_replay_state(&self, successful: bool) {
        self.replay_completed.store(true, Ordering::SeqCst);
        self.replay_successful.store(successful, Ordering::SeqCst);
    }

    pub(crate) fn mark_http_bound(&self) {
        self.http_bound.store(true, Ordering::SeqCst);
    }

    pub(crate) fn clear_http_bound(&self) {
        self.http_bound.store(false, Ordering::SeqCst);
    }

    pub(crate) fn mark_ipc_bound(&self) {
        self.ipc_bound.store(true, Ordering::SeqCst);
    }

    pub(crate) fn clear_ipc_bound(&self) {
        self.ipc_bound.store(false, Ordering::SeqCst);
    }

    pub(crate) fn mark_ipc_stream_bound(&self) {
        self.ipc_stream_bound.store(true, Ordering::SeqCst);
    }

    pub(crate) fn clear_ipc_stream_bound(&self) {
        self.ipc_stream_bound.store(false, Ordering::SeqCst);
    }
}
