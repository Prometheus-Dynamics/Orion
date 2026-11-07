//! TCP settings that make a vanished peer show up within seconds, even on an idle connection.

use std::time::Duration;

use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

/// Most bytes one socket read asks for. tungstenite zero-fills that much of its read buffer
/// before every read, including reads that find nothing (`FrameCodec::read_in` in 0.29). With
/// its default of 128 KiB, an idle poll of a connection cleared 128 KiB, which a board profile
/// showed as 7% of an NT4 client's process in `memset`. NT4 messages are batches of small
/// frames, and 4 KiB covers a typical TCP segment run in one read. Larger messages still work:
/// the buffer grows and fills over several reads.
const READ_CHUNK: usize = 4 * 1024;

/// The WebSocket settings for both the client and the server: tungstenite's defaults with a
/// [`READ_CHUNK`] read buffer (also the buffer's starting size, so a connection holds 4 KiB
/// instead of 128 KiB when idle).
pub(crate) fn ws_config() -> WebSocketConfig {
    WebSocketConfig::default().read_buffer_size(READ_CHUNK)
}

/// Keepalive probes start after this much silence, and repeat this often.
const KEEPALIVE: Duration = Duration::from_secs(1);
/// Unanswered probes before the kernel drops the connection.
const KEEPALIVE_PROBES: u32 = 3;

/// Turns on TCP keepalive (1 s idle, 1 s interval, 3 probes) and, on Linux, `TCP_USER_TIMEOUT`
/// (`timeout`): data the peer does not acknowledge within it fails the connection. Without them
/// a peer whose link dropped (an unplugged USB network gadget, a pulled cable) stays
/// `ESTABLISHED` for minutes. Errors are ignored: the application timeouts still apply.
pub(crate) fn tune(stream: &TcpStream, timeout: Duration) {
    let _ = stream.set_nodelay(true);
    let socket = SockRef::from(stream);
    let keepalive = TcpKeepalive::new()
        .with_time(KEEPALIVE)
        .with_interval(KEEPALIVE);
    #[cfg(not(any(target_os = "windows", target_os = "openbsd", target_os = "redox")))]
    let keepalive = keepalive.with_retries(KEEPALIVE_PROBES);
    let _ = socket.set_tcp_keepalive(&keepalive);
    #[cfg(any(target_os = "linux", target_os = "android", target_os = "fuchsia"))]
    let _ = socket.set_tcp_user_timeout(Some(timeout.max(Duration::from_secs(1))));
    #[cfg(not(any(target_os = "linux", target_os = "android", target_os = "fuchsia")))]
    let _ = timeout;
}
