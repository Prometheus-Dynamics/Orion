use crate::{TcpEndpoint, TcpFrame, TcpStreamTransport};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Inner {
    listeners: BTreeMap<TcpEndpoint, VecDeque<TcpFrame>>,
}

#[derive(Clone, Default)]
pub struct TcpTransport {
    inner: Arc<Mutex<Inner>>,
}

impl TcpTransport {
    pub fn new() -> Self {
        Self::default()
    }
}

impl TcpStreamTransport for TcpTransport {
    fn register_listener(&self, endpoint: TcpEndpoint) -> bool {
        let mut inner = self.inner.lock().expect("tcp listener mutex poisoned");
        inner.listeners.insert(endpoint, VecDeque::new()).is_none()
    }

    fn send_frame(&self, frame: TcpFrame) -> bool {
        let mut inner = self.inner.lock().expect("tcp send mutex poisoned");
        let Some(queue) = inner.listeners.get_mut(&frame.destination) else {
            return false;
        };
        queue.push_back(frame);
        true
    }

    fn recv_frame(&self, endpoint: &TcpEndpoint) -> Option<TcpFrame> {
        let mut inner = self.inner.lock().expect("tcp recv mutex poisoned");
        inner.listeners.get_mut(endpoint)?.pop_front()
    }
}
