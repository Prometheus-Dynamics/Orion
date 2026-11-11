//! One WebSocket connection after the upgrade: whole messages in, whole messages out.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
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
    /// Where a masked frame is put together, kept from one frame to the next.
    out: Vec<u8>,
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
        self.out.clear();
        self.out.extend_from_slice(&head[..len]);
        self.out.extend(
            payload
                .iter()
                .enumerate()
                .map(|(index, byte)| byte ^ mask[index % 4]),
        );
        let sent = self.stream.write_all(&self.out).await;
        // One large message is not a reason to hold that much for good.
        if self.out.capacity() > 64 * 1024 {
            self.out = Vec::new();
        }
        sent
    }
}

/// The reading half of a WebSocket connection.
pub struct WsRead {
    stream: BufReader<OwnedReadHalf>,
    writer: Arc<Mutex<Writer>>,
    /// Whether the other end must mask what it sends: a server's clients must.
    expects_mask: bool,
    /// What [`WsRead::next`] has read so far, kept across calls so that a call may be
    /// dropped half way.
    step: Step,
}

/// What [`WsRead::next`] heard.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Heard {
    /// A whole text message: [`WsRead::message`] is its bytes, in UTF-8.
    Text,
    /// A whole binary message: [`WsRead::message`] is its bytes.
    Binary,
    /// A ping or a pong: no message, but the other end is there.
    Alive,
}

/// Where [`WsRead::next`] is in the stream.
struct Step {
    /// The bytes of the frame header read so far.
    head: [u8; 14],
    head_len: usize,
    /// The frame whose payload is being read, once its header is whole.
    frame: Option<Frame>,
    /// The message so far.
    message: Vec<u8>,
    /// What the message is, from its first frame, while its later frames come.
    kind: Option<u8>,
    /// A control frame's payload.
    control: [u8; 125],
}

impl Default for Step {
    fn default() -> Self {
        Self {
            head: [0; 14],
            head_len: 0,
            frame: None,
            message: Vec::new(),
            kind: None,
            control: [0; 125],
        }
    }
}

struct Frame {
    opcode: u8,
    last: bool,
    len: usize,
    mask: Option<[u8; 4]>,
    done: usize,
}

impl std::fmt::Debug for WsRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WsRead")
    }
}

/// The writing half of a WebSocket connection.
pub struct WsWrite {
    writer: Arc<Mutex<Writer>>,
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

impl WsRead {
    /// How long the header of the frame being read is, as far as can be told from what
    /// of it has come.
    fn head_need(&self) -> usize {
        if self.step.head_len < 2 {
            return 2;
        }
        let extended = match self.step.head[1] & 0x7F {
            126 => 2,
            127 => 8,
            _ => 0,
        };
        let mask = if self.step.head[1] & 0x80 != 0 { 4 } else { 0 };
        2 + extended + mask
    }

    /// Reads the header of the next frame. `None` if the other end is gone between
    /// messages.
    async fn head(&mut self, most: usize) -> io::Result<Option<Frame>> {
        loop {
            let need = self.head_need();
            if self.step.head_len >= 2 && self.step.head_len == need {
                break;
            }
            let available = self.stream.fill_buf().await?;
            if available.is_empty() {
                return if self.step.head_len == 0 && self.step.kind.is_none() {
                    Ok(None)
                } else {
                    Err(io::ErrorKind::UnexpectedEof.into())
                };
            }
            let take = (need - self.step.head_len).min(available.len());
            self.step.head[self.step.head_len..self.step.head_len + take]
                .copy_from_slice(&available[..take]);
            self.stream.consume(take);
            self.step.head_len += take;
        }
        let head = self.step.head;
        self.step.head_len = 0;
        let (last, opcode) = (head[0] & 0x80 != 0, head[0] & 0x0F);
        if head[0] & 0x70 != 0 {
            return Err(broken("an extension nobody agreed to"));
        }
        let masked = head[1] & 0x80 != 0;
        if masked != self.expects_mask {
            return Err(broken("a frame masked the wrong way round"));
        }
        let (len, at) = match head[1] & 0x7F {
            126 => (u64::from(u16::from_be_bytes([head[2], head[3]])), 4),
            127 => {
                let mut len = [0_u8; 8];
                len.copy_from_slice(&head[2..10]);
                (u64::from_be_bytes(len), 10)
            }
            short => (u64::from(short), 2),
        };
        let len = usize::try_from(len).map_err(|_| broken("a message too long"))?;
        let mask = masked.then(|| [head[at], head[at + 1], head[at + 2], head[at + 3]]);
        if opcode & 0x8 != 0 {
            if len > 125 || !last {
                return Err(broken("a control frame that is too long or in pieces"));
            }
        } else {
            match (opcode, self.step.kind) {
                (TEXT | BINARY, None) => {
                    self.step.kind = Some(opcode);
                    // One long message is not a reason to hold that much for good.
                    if self.step.message.capacity() > 64 * 1024 {
                        self.step.message = Vec::new();
                    }
                    self.step.message.clear();
                }
                (CONTINUATION, Some(_)) => {}
                _ => return Err(broken("frames out of order")),
            }
            if self
                .step
                .message
                .len()
                .checked_add(len)
                .is_none_or(|end| end > most)
            {
                return Err(broken("a message longer than is taken"));
            }
        }
        Ok(Some(Frame {
            opcode,
            last,
            len,
            mask,
            done: 0,
        }))
    }

    /// Waits for the next whole message, of at most `most` bytes, and keeps it:
    /// [`WsRead::message`] is its bytes until the next call. `None` when the other end has
    /// closed. Pings are answered here.
    ///
    /// Unlike [`WsRead::recv`], a call may be dropped before it is done, as `select!` does,
    /// and the next call carries on from where it was. Use one or the other on a connection.
    ///
    /// # Errors
    ///
    /// If the connection breaks, the other end does not keep to the protocol, or a message
    /// is longer than `most`.
    pub async fn next(&mut self, most: usize) -> io::Result<Option<Heard>> {
        loop {
            if self.step.frame.is_none() {
                let Some(frame) = self.head(most).await? else {
                    return Ok(None);
                };
                self.step.frame = Some(frame);
            }
            while let Some(frame) = &mut self.step.frame {
                if frame.done == frame.len {
                    break;
                }
                let available = self.stream.fill_buf().await?;
                if available.is_empty() {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
                let take = (frame.len - frame.done).min(available.len());
                let into = if frame.opcode & 0x8 != 0 {
                    let into = &mut self.step.control[frame.done..frame.done + take];
                    into.copy_from_slice(&available[..take]);
                    into
                } else {
                    let from = self.step.message.len();
                    self.step.message.extend_from_slice(&available[..take]);
                    &mut self.step.message[from..]
                };
                if let Some(mask) = frame.mask {
                    for (index, byte) in into.iter_mut().enumerate() {
                        *byte ^= mask[(frame.done + index) % 4];
                    }
                }
                frame.done += take;
                self.stream.consume(take);
            }
            let Some(frame) = self.step.frame.take() else {
                continue;
            };
            match frame.opcode {
                PING | CLOSE => {
                    // Answered from a task of its own: a write cut short by this call
                    // being dropped would break the stream for good.
                    let (writer, payload) = (self.writer.clone(), self.step.control);
                    let answer = if frame.opcode == PING { PONG } else { CLOSE };
                    tokio::spawn(async move {
                        let _ = writer
                            .lock()
                            .await
                            .frame(answer, &payload[..frame.len])
                            .await;
                    });
                    if frame.opcode == CLOSE {
                        return Ok(None);
                    }
                    return Ok(Some(Heard::Alive));
                }
                PONG => return Ok(Some(Heard::Alive)),
                TEXT | BINARY | CONTINUATION => {
                    if frame.last {
                        let kind = self.step.kind.take();
                        return Ok(Some(if kind == Some(TEXT) {
                            Heard::Text
                        } else {
                            Heard::Binary
                        }));
                    }
                }
                _ => return Err(broken("an unknown control frame")),
            }
        }
    }

    /// The message [`WsRead::next`] last gave.
    #[must_use]
    pub fn message(&self) -> &[u8] {
        &self.step.message
    }
}

impl WsWrite {
    /// Asks the other end to show it is there: it answers with a pong.
    ///
    /// # Errors
    ///
    /// If the connection is broken.
    pub async fn ping(&mut self) -> io::Result<()> {
        self.writer.lock().await.frame(PING, &[]).await
    }

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
        let writer = Arc::new(Mutex::new(Writer {
            stream: write,
            masks: !server,
            masked: 0x9E37_79B9,
            out: Vec::new(),
        }));
        Self {
            read: WsRead {
                stream: read,
                writer: writer.clone(),
                expects_mask: server,
                step: Step::default(),
            },
            write: WsWrite { writer },
            peer_addr,
            path,
            origin,
            protocol,
        }
    }
}
