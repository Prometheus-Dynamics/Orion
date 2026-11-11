//! One WebSocket connection after the upgrade: whole messages in, whole messages out.

use std::io;
use std::net::SocketAddr;
use std::rc::Rc;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;

/// What a message is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Message {
    /// Bytes: so many of them are in the buffer.
    Binary(usize),
    /// Text, in UTF-8: so many bytes of it are in the buffer.
    Text(usize),
}

const CONTINUATION: u8 = 0x0;
const TEXT: u8 = 0x1;
const BINARY: u8 = 0x2;
const CLOSE: u8 = 0x8;
const PING: u8 = 0x9;
const PONG: u8 = 0xA;

fn broken(why: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, why)
}

/// The writing end, which both halves use: the reading half answers pings with it.
struct Writer {
    stream: OwnedWriteHalf,
    /// Whether frames this end sends are masked: a client's are, a server's are not.
    masks: bool,
    /// A counter that makes each mask different. The mask is not a secret.
    masked: u32,
}

impl Writer {
    async fn frame(&mut self, opcode: u8, payload: &[u8]) -> io::Result<()> {
        let mut head = [0_u8; 14];
        head[0] = 0x80 | opcode;
        let mask_bit = if self.masks { 0x80 } else { 0 };
        let mut len = match payload.len() {
            small @ 0..=125 => {
                head[1] = mask_bit | small as u8;
                2
            }
            medium @ 126..=65_535 => {
                head[1] = mask_bit | 126;
                head[2..4].copy_from_slice(&(medium as u16).to_be_bytes());
                4
            }
            large => {
                head[1] = mask_bit | 127;
                head[2..10].copy_from_slice(&(large as u64).to_be_bytes());
                10
            }
        };
        if !self.masks {
            self.stream.write_all(&head[..len]).await?;
            return self.stream.write_all(payload).await;
        }
        self.masked = self
            .masked
            .wrapping_mul(1_664_525)
            .wrapping_add(1_013_904_223);
        let mask = self.masked.to_be_bytes();
        head[len..len + 4].copy_from_slice(&mask);
        len += 4;
        let mut out = Vec::with_capacity(len + payload.len());
        out.extend_from_slice(&head[..len]);
        out.extend(
            payload
                .iter()
                .enumerate()
                .map(|(index, byte)| byte ^ mask[index % 4]),
        );
        self.stream.write_all(&out).await
    }
}

/// The reading half of a WebSocket connection.
pub struct WsRead {
    stream: BufReader<OwnedReadHalf>,
    writer: Rc<Mutex<Writer>>,
    /// Whether the other end must mask what it sends: a server's clients must.
    expects_mask: bool,
}

impl std::fmt::Debug for WsRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WsRead")
    }
}

/// The writing half of a WebSocket connection.
pub struct WsWrite {
    writer: Rc<Mutex<Writer>>,
}

impl std::fmt::Debug for WsWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WsWrite")
    }
}

impl WsRead {
    /// Reads the next whole message into `buf`. `None` when the other end has closed.
    /// Pings are answered here and never returned.
    ///
    /// # Errors
    ///
    /// If the connection breaks, the other end does not keep to the protocol, or the
    /// message is longer than `buf`: it is not buffered past that.
    pub async fn recv(&mut self, buf: &mut [u8]) -> io::Result<Option<Message>> {
        let mut filled = 0_usize;
        // What the message is, from its first frame, while its later frames come.
        let mut kind = None;
        loop {
            let mut head = [0_u8; 2];
            if let Err(error) = self.stream.read_exact(&mut head).await {
                // Gone between messages without saying so: closed.
                return if error.kind() == io::ErrorKind::UnexpectedEof && kind.is_none() {
                    Ok(None)
                } else {
                    Err(error)
                };
            }
            let (last, opcode) = (head[0] & 0x80 != 0, head[0] & 0x0F);
            if head[0] & 0x70 != 0 {
                return Err(broken("an extension nobody agreed to"));
            }
            let masked = head[1] & 0x80 != 0;
            if masked != self.expects_mask {
                return Err(broken("a frame masked the wrong way round"));
            }
            let len = match head[1] & 0x7F {
                126 => u64::from(self.stream.read_u16().await?),
                127 => self.stream.read_u64().await?,
                short => u64::from(short),
            };
            let mut mask = [0_u8; 4];
            if masked {
                self.stream.read_exact(&mut mask).await?;
            }
            let control = opcode & 0x8 != 0;
            if control {
                // Control frames are short, whole, and may come between the frames of a
                // message.
                if len > 125 || !last {
                    return Err(broken("a control frame that is too long or in pieces"));
                }
                let mut payload = [0_u8; 125];
                let payload = &mut payload[..len as usize];
                self.stream.read_exact(payload).await?;
                for (index, byte) in payload.iter_mut().enumerate() {
                    *byte ^= mask[index % 4];
                }
                match opcode {
                    PING => self.writer.lock().await.frame(PONG, payload).await?,
                    PONG => {}
                    CLOSE => {
                        // Said back, and that is the end.
                        let _ = self.writer.lock().await.frame(CLOSE, payload).await;
                        return Ok(None);
                    }
                    _ => return Err(broken("an unknown control frame")),
                }
                continue;
            }
            match (opcode, kind) {
                (TEXT | BINARY, None) => kind = Some(opcode),
                (CONTINUATION, Some(_)) => {}
                _ => return Err(broken("frames out of order")),
            }
            let len = usize::try_from(len).map_err(|_| broken("a message too long"))?;
            let end = filled
                .checked_add(len)
                .filter(|end| *end <= buf.len())
                .ok_or_else(|| broken("a message longer than is taken"))?;
            self.stream.read_exact(&mut buf[filled..end]).await?;
            if masked {
                for (index, byte) in buf[filled..end].iter_mut().enumerate() {
                    *byte ^= mask[index % 4];
                }
            }
            filled = end;
            if last {
                return Ok(Some(if kind == Some(TEXT) {
                    Message::Text(filled)
                } else {
                    Message::Binary(filled)
                }));
            }
        }
    }
}

impl WsWrite {
    /// Sends `payload` as one binary message.
    ///
    /// # Errors
    ///
    /// If the connection is broken.
    pub async fn send_binary(&mut self, payload: &[u8]) -> io::Result<()> {
        self.writer.lock().await.frame(BINARY, payload).await
    }

    /// Sends `text` as one text message.
    ///
    /// # Errors
    ///
    /// If the connection is broken.
    pub async fn send_text(&mut self, text: &str) -> io::Result<()> {
        self.writer.lock().await.frame(TEXT, text.as_bytes()).await
    }

    /// Says goodbye. The other end answers and the connection ends.
    ///
    /// # Errors
    ///
    /// If the connection is broken.
    pub async fn close(&mut self) -> io::Result<()> {
        // 1000: a normal end.
        self.writer
            .lock()
            .await
            .frame(CLOSE, &1000_u16.to_be_bytes())
            .await
    }
}

/// A WebSocket connection, after the upgrade.
#[derive(Debug)]
pub struct WsSocket {
    /// The reading half.
    pub read: WsRead,
    /// The writing half.
    pub write: WsWrite,
    /// The other end's address.
    pub peer_addr: SocketAddr,
    /// The path that was asked for, such as `/orion`.
    pub path: String,
    /// The page the connection came from, if the other end said: a browser does.
    pub origin: Option<String>,
    /// The subprotocol both ends settled on.
    pub protocol: String,
}

impl WsSocket {
    /// Makes a socket of the two halves of a connection that has been upgraded. `server`
    /// says which end this is: a server's frames are not masked, and its clients' are.
    pub(crate) fn new(
        read: BufReader<OwnedReadHalf>,
        write: OwnedWriteHalf,
        server: bool,
        peer_addr: SocketAddr,
        path: String,
        origin: Option<String>,
        protocol: String,
    ) -> Self {
        let writer = Rc::new(Mutex::new(Writer {
            stream: write,
            masks: !server,
            masked: 0x9E37_79B9,
        }));
        Self {
            read: WsRead {
                stream: read,
                writer: writer.clone(),
                expects_mask: server,
            },
            write: WsWrite { writer },
            peer_addr,
            path,
            origin,
            protocol,
        }
    }
}
