use crate::{
    ControlEnvelope, DataEnvelope, LocalAddress, LocalControlTransport, LocalDataTransport,
};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

const DEFAULT_MEMORY_QUEUE_CAPACITY: usize = 1024;

#[derive(Default)]
struct Inner {
    control_endpoints: BTreeMap<LocalAddress, VecDeque<ControlEnvelope>>,
    data_endpoints: BTreeMap<LocalAddress, VecDeque<DataEnvelope>>,
}

#[derive(Clone)]
pub struct IpcTransport {
    inner: Arc<Mutex<Inner>>,
    queue_capacity: usize,
}

impl Default for IpcTransport {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
            queue_capacity: DEFAULT_MEMORY_QUEUE_CAPACITY,
        }
    }
}

impl IpcTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_queue_capacity(mut self, queue_capacity: usize) -> Self {
        self.queue_capacity = queue_capacity.max(1);
        self
    }
}

impl LocalControlTransport for IpcTransport {
    fn register_control_endpoint(&self, address: LocalAddress) -> bool {
        let mut inner = self.inner.lock().expect("control endpoint mutex poisoned");
        inner
            .control_endpoints
            .insert(address, VecDeque::new())
            .is_none()
    }

    fn send_control(&self, envelope: ControlEnvelope) -> bool {
        let mut inner = self.inner.lock().expect("control send mutex poisoned");
        let Some(queue) = inner.control_endpoints.get_mut(&envelope.destination) else {
            return false;
        };
        if queue.len() >= self.queue_capacity {
            return false;
        }
        queue.push_back(envelope);
        true
    }

    fn recv_control(&self, address: &LocalAddress) -> Option<ControlEnvelope> {
        let mut inner = self.inner.lock().expect("control recv mutex poisoned");
        inner.control_endpoints.get_mut(address)?.pop_front()
    }
}

impl LocalDataTransport for IpcTransport {
    fn register_data_endpoint(&self, address: LocalAddress) -> bool {
        let mut inner = self.inner.lock().expect("data endpoint mutex poisoned");
        inner
            .data_endpoints
            .insert(address, VecDeque::new())
            .is_none()
    }

    fn send_data(&self, envelope: DataEnvelope) -> bool {
        let mut inner = self.inner.lock().expect("data send mutex poisoned");
        let Some(queue) = inner.data_endpoints.get_mut(&envelope.destination) else {
            return false;
        };
        if queue.len() >= self.queue_capacity {
            return false;
        }
        queue.push_back(envelope);
        true
    }

    fn recv_data(&self, address: &LocalAddress) -> Option<DataEnvelope> {
        let mut inner = self.inner.lock().expect("data recv mutex poisoned");
        inner.data_endpoints.get_mut(address)?.pop_front()
    }
}
