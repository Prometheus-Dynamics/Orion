//! The page the camera looks at: a small HTTP server for the page, and a WebSocket that carries
//! flip commands from the tool and paint and ping replies back.
//!
//! Wire format, JSON text frames. Page times are `performance.now()` in microseconds.
//!
//! * tool -> page: `{"t":"ping","st":<tool us>}` and `{"t":"flip","seq":<n>,"on":<bool>}`
//! * page -> tool: `{"t":"pong","st":<echoed>,"pc":<page us>}` and `{"t":"painted","seq":<n>,"pc":<page us>}`

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use orion_nt4::now_micros;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

/// What the page reports back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PageEvent {
    Connected,
    Disconnected,
    /// `sent_us` and `received_us` are on the tool clock; `page_us` on the page's.
    Pong {
        sent_us: i64,
        received_us: i64,
        page_us: i64,
    },
    /// The page applied flip `seq` in the frame that began at `page_us`.
    Painted {
        seq: u32,
        page_us: i64,
    },
}

/// The running display. Dropping it stops accepting pages.
pub struct Display {
    pub local_addr: SocketAddr,
    pub events: mpsc::UnboundedReceiver<PageEvent>,
    commands: broadcast::Sender<String>,
    accept: JoinHandle<()>,
}

struct Context {
    html: String,
    commands: broadcast::Sender<String>,
    events: mpsc::UnboundedSender<PageEvent>,
}

impl Display {
    /// Serves `html` at `/` and the WebSocket at `/ws` on `bind`.
    pub async fn start(bind: SocketAddr, html: String) -> io::Result<Self> {
        let listener = TcpListener::bind(bind).await?;
        let local_addr = listener.local_addr()?;
        let (commands, _) = broadcast::channel(64);
        let (events_tx, events) = mpsc::unbounded_channel();
        let context = Arc::new(Context {
            html,
            commands: commands.clone(),
            events: events_tx,
        });
        let accept = tokio::spawn(accept_loop(listener, context));
        Ok(Self {
            local_addr,
            events,
            commands,
            accept,
        })
    }

    /// Sends a ping. `sent_us` must be the tool clock just before the call.
    pub fn ping(&self, sent_us: i64) {
        let _ = self
            .commands
            .send(json!({"t": "ping", "st": sent_us}).to_string());
    }

    /// Asks the page to show the tag (`on`) or a black screen, as flip `seq`.
    pub fn flip(&self, seq: u32, on: bool) {
        let _ = self
            .commands
            .send(json!({"t": "flip", "seq": seq, "on": on}).to_string());
    }
}

impl Drop for Display {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

async fn accept_loop(listener: TcpListener, context: Arc<Context>) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(serve(stream, Arc::clone(&context)));
            }
            // Typically EMFILE: back off instead of spinning.
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

async fn serve(mut stream: TcpStream, context: Arc<Context>) {
    let Ok(head) = read_head(&mut stream).await else {
        return;
    };
    let request = String::from_utf8_lossy(&head);
    let target = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");
    let path = target.split('?').next().unwrap_or("/");
    if path == "/ws" {
        // accept_async reads the request itself; read_head only peeked at it.
        if let Ok(ws) = tokio_tungstenite::accept_async(stream).await {
            page_session(ws, context).await;
        }
        return;
    }
    // Consume the request head, then answer.
    let mut consumed = vec![0u8; head.len()];
    if stream.read_exact(&mut consumed).await.is_err() {
        return;
    }
    let response = if path == "/" || path == "/index.html" {
        http_response("200 OK", "text/html; charset=utf-8", &context.html)
    } else {
        http_response("404 Not Found", "text/plain; charset=utf-8", "not found")
    };
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}

/// Waits until the request head (through the blank line) is buffered, without consuming it.
async fn read_head(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    const MAX: usize = 8192;
    let mut buf = vec![0u8; MAX];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let n = stream.peek(&mut buf).await?;
        if let Some(pos) = buf[..n].windows(4).position(|w| w == b"\r\n\r\n") {
            return Ok(buf[..pos + 4].to_vec());
        }
        if n == 0 || n == MAX || tokio::time::Instant::now() > deadline {
            return Err(io::Error::other("bad or slow request head"));
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

fn http_response(status: &str, content_type: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
         Cache-Control: no-store\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

async fn page_session(ws: WebSocketStream<TcpStream>, context: Arc<Context>) {
    let (mut sink, mut source) = ws.split();
    let mut commands = context.commands.subscribe();
    let _ = context.events.send(PageEvent::Connected);
    loop {
        tokio::select! {
            command = commands.recv() => match command {
                Ok(text) => {
                    if sink.send(Message::Text(text.into())).await.is_err() {
                        break;
                    }
                }
                // A slow page misses old commands; the newest ones still arrive.
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => break,
            },
            incoming = source.next() => match incoming {
                Some(Ok(Message::Text(text))) => {
                    if let Some(event) = parse_page_message(text.as_str(), now_micros()) {
                        let _ = context.events.send(event);
                    }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                Some(Ok(_)) => {}
            },
        }
    }
    let _ = context.events.send(PageEvent::Disconnected);
}

/// Decodes one page message. Anything malformed is ignored.
pub fn parse_page_message(text: &str, received_us: i64) -> Option<PageEvent> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    let page_us = value.get("pc")?.as_f64()?.round() as i64;
    match value.get("t")?.as_str()? {
        "pong" => Some(PageEvent::Pong {
            sent_us: value.get("st")?.as_f64()?.round() as i64,
            received_us,
            page_us,
        }),
        "painted" => Some(PageEvent::Painted {
            seq: u32::try_from(value.get("seq")?.as_u64()?).ok()?,
            page_us,
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_messages_parse() {
        assert_eq!(
            parse_page_message(r#"{"t":"pong","st":100,"pc":2500.4}"#, 400),
            Some(PageEvent::Pong {
                sent_us: 100,
                received_us: 400,
                page_us: 2500
            })
        );
        assert_eq!(
            parse_page_message(r#"{"t":"painted","seq":3,"pc":9000.0}"#, 0),
            Some(PageEvent::Painted {
                seq: 3,
                page_us: 9000
            })
        );
        assert_eq!(parse_page_message("not json", 0), None);
        assert_eq!(parse_page_message(r#"{"t":"unknown","pc":1}"#, 0), None);
        assert_eq!(
            parse_page_message(r#"{"t":"painted","seq":-1,"pc":1}"#, 0),
            None
        );
    }
}
