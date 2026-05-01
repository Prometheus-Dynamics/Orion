use crate::{TcpEndpoint, TcpFrame};

pub trait TcpStreamTransport {
    fn register_listener(&self, endpoint: TcpEndpoint) -> bool;

    fn send_frame(&self, frame: TcpFrame) -> bool;

    fn recv_frame(&self, endpoint: &TcpEndpoint) -> Option<TcpFrame>;
}
