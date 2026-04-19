use crate::{QuicEndpoint, QuicFrame, QuicTransportAdapter};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Inner {
    listeners: BTreeMap<QuicEndpoint, VecDeque<QuicFrame>>,
}

#[derive(Clone, Default)]
pub struct QuicTransport {
    inner: Arc<Mutex<Inner>>,
}

impl QuicTransport {
    pub fn new() -> Self {
        Self::default()
    }
}

impl QuicTransportAdapter for QuicTransport {
    fn register_listener(&self, endpoint: QuicEndpoint) -> bool {
        let mut inner = self.inner.lock().expect("quic listener mutex poisoned");
        inner.listeners.insert(endpoint, VecDeque::new()).is_none()
    }

    fn send_frame(&self, frame: QuicFrame) -> bool {
        let mut inner = self.inner.lock().expect("quic send mutex poisoned");
        let Some(queue) = inner.listeners.get_mut(&frame.destination) else {
            return false;
        };
        queue.push_back(frame);
        true
    }

    fn recv_frame(&self, endpoint: &QuicEndpoint) -> Option<QuicFrame> {
        let mut inner = self.inner.lock().expect("quic recv mutex poisoned");
        inner.listeners.get_mut(endpoint)?.pop_front()
    }
}
