use crate::{QuicEndpoint, QuicFrame};

pub trait QuicTransportAdapter {
    fn register_listener(&self, endpoint: QuicEndpoint) -> bool;

    fn send_frame(&self, frame: QuicFrame) -> bool;

    fn recv_frame(&self, endpoint: &QuicEndpoint) -> Option<QuicFrame>;
}
