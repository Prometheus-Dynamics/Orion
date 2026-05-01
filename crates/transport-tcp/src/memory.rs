use crate::{TcpEndpoint, TcpFrame, TcpStreamTransport};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

const DEFAULT_MEMORY_QUEUE_CAPACITY: usize = 1024;

#[derive(Default)]
struct Inner {
    listeners: BTreeMap<TcpEndpoint, VecDeque<TcpFrame>>,
}

#[derive(Clone)]
pub struct TcpTransport {
    inner: Arc<Mutex<Inner>>,
    queue_capacity: usize,
}

impl Default for TcpTransport {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
            queue_capacity: DEFAULT_MEMORY_QUEUE_CAPACITY,
        }
    }
}

impl TcpTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_queue_capacity(mut self, queue_capacity: usize) -> Self {
        self.queue_capacity = queue_capacity.max(1);
        self
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
        if queue.len() >= self.queue_capacity {
            return false;
        }
        queue.push_back(frame);
        true
    }

    fn recv_frame(&self, endpoint: &TcpEndpoint) -> Option<TcpFrame> {
        let mut inner = self.inner.lock().expect("tcp recv mutex poisoned");
        inner.listeners.get_mut(endpoint)?.pop_front()
    }
}
