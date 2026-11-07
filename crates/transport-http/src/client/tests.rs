//! The client's URL parsing and HTTP/1.1 exchanges against scripted servers.

use super::*;

#[test]
fn base_urls_parse() {
    let t = Target::parse("http://127.0.0.1:9100/").unwrap();
    assert_eq!(
        (
            t.scheme,
            t.host.as_str(),
            t.port,
            t.authority.as_str(),
            t.prefix.as_str()
        ),
        (Scheme::Http, "127.0.0.1", 9100, "127.0.0.1:9100", "")
    );
    assert_eq!(t.base_url, "http://127.0.0.1:9100");
    let t = Target::parse("https://[::1]:8443/orion/").unwrap();
    assert_eq!(
        (t.host.as_str(), t.port, t.prefix.as_str()),
        ("::1", 8443, "/orion")
    );
    assert_eq!(t.authority, "[::1]:8443");
    let t = Target::parse("HTTPS://Node-A.local").unwrap();
    assert_eq!((t.host.as_str(), t.port), ("node-a.local", 443));
    for bad in [
        "127.0.0.1:9100",
        "ftp://host",
        "http://",
        "http://host:port",
        "http://[::1",
        "http://user@host",
        "http://host/?q",
    ] {
        assert!(
            matches!(
                Target::parse(bad),
                Err(HttpTransportError::InvalidBaseUrl(_))
            ),
            "{bad}"
        );
    }
}

/// A scripted HTTP/1.1 server: for each accepted connection, the replies to send, one per
/// request read; a connection is closed after its last reply. Returns the address and the
/// count of accepted connections.
async fn scripted(
    connections: Vec<Vec<&'static [u8]>>,
) -> (std::net::SocketAddr, Arc<std::sync::atomic::AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = accepted.clone();
    tokio::spawn(async move {
        for replies in connections {
            let (mut stream, _) = listener.accept().await.unwrap();
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::spawn(async move {
                let mut buf = Vec::new();
                for reply in replies {
                    // One request: its head, then `content-length` bytes.
                    loop {
                        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            let head = std::str::from_utf8(&buf[..end]).unwrap().to_owned();
                            let len: usize = head
                                .lines()
                                .find_map(|l| l.strip_prefix("content-length: "))
                                .map_or(0, |v| v.parse().unwrap());
                            if buf.len() >= end + 4 + len {
                                buf.drain(..end + 4 + len);
                                break;
                            }
                        }
                        let mut chunk = [0u8; 1024];
                        let n = stream.read(&mut chunk).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                    }
                    stream.write_all(reply).await.unwrap();
                }
            });
        }
    });
    (addr, accepted)
}

fn client(addr: std::net::SocketAddr) -> HttpClient {
    HttpClient::try_new(format!("http://{addr}")).unwrap()
}

async fn get(client: &HttpClient) -> Result<Response, HttpTransportError> {
    let head = client.inner.request_head(HttpMethod::Get, "/health", 0);
    client.inner.round_trip(&head, &[]).await
}

const OK: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok";

#[tokio::test]
async fn a_kept_alive_connection_is_reused() {
    let (addr, accepted) = scripted(vec![vec![OK, OK, OK]]).await;
    let client = client(addr);
    for _ in 0..3 {
        let response = get(&client).await.unwrap();
        assert_eq!((response.status, &response.body[..]), (200, &b"ok"[..]));
    }
    assert_eq!(accepted.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_connection_the_server_closed_is_replaced() {
    // The first connection answers once and closes; the request after it goes on a new one.
    let (addr, accepted) = scripted(vec![vec![OK], vec![OK]]).await;
    let client = client(addr);
    get(&client).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let response = get(&client).await.unwrap();
    assert_eq!(response.body, b"ok");
    assert_eq!(accepted.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[tokio::test]
async fn chunked_and_closing_responses_are_read() {
    let chunked: &[u8] = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n\
        3;ext=1\r\nabc\r\n5\r\ndefgh\r\n0\r\ntrailer: x\r\n\r\n";
    let closing: &[u8] = b"HTTP/1.1 503 Service Unavailable\r\nconnection: close\r\n\
        content-length: 4\r\n\r\nbusy";
    let (addr, accepted) = scripted(vec![vec![chunked, closing], vec![OK]]).await;
    let client = client(addr);
    let response = get(&client).await.unwrap();
    assert_eq!(response.body, b"abcdefgh");
    assert!(response.keep_alive);
    let response = get(&client).await.unwrap();
    assert_eq!((response.status, &response.body[..]), (503, &b"busy"[..]));
    assert!(!response.keep_alive);
    get(&client).await.unwrap();
    assert_eq!(accepted.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[tokio::test]
async fn malformed_responses_are_errors() {
    let (addr, _) = scripted(vec![vec![b"SMTP ready\r\n\r\n"]]).await;
    assert!(matches!(
        get(&client(addr)).await,
        Err(HttpTransportError::RequestFailed { .. })
    ));
}

#[tokio::test]
async fn a_silent_server_times_out_and_a_closed_port_is_connectivity() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = client(addr);
    let error = client
        .get_route(ControlRoute::Health)
        .await
        .expect_err("no answer");
    assert!(
        matches!(
            error,
            HttpTransportError::RequestFailed {
                kind: HttpRequestFailureKind::Timeout,
                ..
            }
        ),
        "{error:?}"
    );
    drop(listener);
    let error = client
        .get_route(ControlRoute::Health)
        .await
        .expect_err("refused");
    assert!(
        matches!(
            error,
            HttpTransportError::RequestFailed {
                kind: HttpRequestFailureKind::Connectivity,
                ..
            }
        ),
        "{error:?}"
    );
}
