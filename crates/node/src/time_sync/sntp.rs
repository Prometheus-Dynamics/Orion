//! SNTP (RFC 4330): the 48-byte NTP packet, a client query and the node's own time service.
//!
//! The same packet serves both sources of the time sync: an internet or site NTP server, and
//! another Orion node that serves its clock (`ORION_NODE_TIME_SERVE`). A query is one datagram
//! out and one back, so serving is not an amplifier.

use std::io;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::net::UdpSocket;

/// Seconds from the NTP epoch (1900) to the Unix epoch (1970).
const NTP_UNIX_OFFSET_S: u64 = 2_208_988_800;
pub(crate) const PACKET_LEN: usize = 48;
const VERSION: u8 = 4;
const MODE_CLIENT: u8 = 3;
const MODE_SERVER: u8 = 4;
/// Leap indicator 3: the server's clock is not synchronized.
const LEAP_ALARM: u8 = 3;
/// How long a query waits for its answer.
const QUERY_TIMEOUT: Duration = Duration::from_secs(2);

/// An NTP timestamp: seconds since 1900 in the high 32 bits, the fraction in the low 32.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct NtpTime(pub(crate) u64);

impl NtpTime {
    pub(crate) fn from_unix_ns(ns: i128) -> Self {
        let ns = ns.max(0) as u128;
        let secs = (ns / 1_000_000_000) as u64 + NTP_UNIX_OFFSET_S;
        let frac = ((ns % 1_000_000_000) << 32) / 1_000_000_000;
        Self((secs << 32) | frac as u64)
    }

    pub(crate) fn to_unix_ns(self) -> i128 {
        let secs = i128::from(self.0 >> 32) - i128::from(NTP_UNIX_OFFSET_S);
        let frac = i128::from(self.0 & 0xFFFF_FFFF);
        secs * 1_000_000_000 + ((frac * 1_000_000_000) >> 32)
    }

    pub(crate) fn now() -> Self {
        Self::from_unix_ns(unix_now_ns())
    }
}

/// The wall clock in nanoseconds since the Unix epoch (negative before it).
pub(crate) fn unix_now_ns() -> i128 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(since) => since.as_nanos() as i128,
        Err(before) => -(before.duration().as_nanos() as i128),
    }
}

/// The fields of an NTP packet the time sync uses.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Packet {
    pub(crate) leap: u8,
    pub(crate) mode: u8,
    pub(crate) stratum: u8,
    pub(crate) reference_id: [u8; 4],
    pub(crate) origin: NtpTime,
    pub(crate) receive: NtpTime,
    pub(crate) transmit: NtpTime,
}

impl Packet {
    pub(crate) fn encode(&self) -> [u8; PACKET_LEN] {
        let mut out = [0u8; PACKET_LEN];
        out[0] = (self.leap << 6) | (VERSION << 3) | self.mode;
        out[1] = self.stratum;
        out[2] = 6; // poll: 64 s
        out[3] = 0xEC; // precision: about a microsecond
        out[12..16].copy_from_slice(&self.reference_id);
        // The reference time is when the clock was last set; the receive time is as good here.
        out[16..24].copy_from_slice(&self.receive.0.to_be_bytes());
        out[24..32].copy_from_slice(&self.origin.0.to_be_bytes());
        out[32..40].copy_from_slice(&self.receive.0.to_be_bytes());
        out[40..48].copy_from_slice(&self.transmit.0.to_be_bytes());
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        let bytes = bytes.get(..PACKET_LEN)?;
        let word = |at: usize| {
            let mut be = [0u8; 8];
            be.copy_from_slice(&bytes[at..at + 8]);
            NtpTime(u64::from_be_bytes(be))
        };
        let mut reference_id = [0u8; 4];
        reference_id.copy_from_slice(&bytes[12..16]);
        Some(Self {
            leap: bytes[0] >> 6,
            mode: bytes[0] & 0x07,
            stratum: bytes[1],
            reference_id,
            origin: word(24),
            receive: word(32),
            transmit: word(40),
        })
    }
}

/// One answered query: `offset` is server time minus local time, `delay` the round trip less
/// the server's own processing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Sample {
    pub(crate) offset_ns: i64,
    pub(crate) delay_ns: i64,
    pub(crate) stratum: u8,
}

/// Why a server's answer was not used.
#[derive(Debug)]
pub(crate) enum QueryError {
    Io(io::Error),
    Timeout,
    /// Not an answer to this query, a kiss-of-death, or a server that is not synchronized.
    Refused(&'static str),
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::Timeout => write!(f, "no answer within {} s", QUERY_TIMEOUT.as_secs()),
            Self::Refused(why) => f.write_str(why),
        }
    }
}

/// Offset and delay from the four timestamps (RFC 4330 section 5).
pub(crate) fn sample(t1: NtpTime, answer: &Packet, t4: NtpTime) -> Result<Sample, QueryError> {
    if answer.mode != MODE_SERVER {
        return Err(QueryError::Refused("not a server answer"));
    }
    if answer.origin != t1 {
        return Err(QueryError::Refused("answer to another query"));
    }
    if answer.stratum == 0 {
        return Err(QueryError::Refused("kiss-of-death from the server"));
    }
    if answer.leap == LEAP_ALARM || answer.stratum > 15 {
        return Err(QueryError::Refused("the server is not synchronized"));
    }
    let (t1, t2, t3, t4) = (
        t1.to_unix_ns(),
        answer.receive.to_unix_ns(),
        answer.transmit.to_unix_ns(),
        t4.to_unix_ns(),
    );
    let offset = ((t2 - t1) + (t3 - t4)) / 2;
    let delay = (t4 - t1) - (t3 - t2);
    Ok(Sample {
        offset_ns: offset.clamp(i64::MIN.into(), i64::MAX.into()) as i64,
        delay_ns: delay.clamp(0, i64::MAX.into()) as i64,
        stratum: answer.stratum,
    })
}

/// How a source is reached: SNTP over UDP, or the same 48 bytes each way over a TCP connection
/// (an Orion node's time service accepts both, so time also crosses TCP-only paths such as an
/// SSH tunnel).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Carrier {
    Udp,
    Tcp,
}

/// Asks `server` for the time once.
pub(crate) async fn query_over(carrier: Carrier, server: SocketAddr) -> Result<Sample, QueryError> {
    match carrier {
        Carrier::Udp => query(server).await,
        Carrier::Tcp => tokio::time::timeout(QUERY_TIMEOUT, query_tcp(server))
            .await
            .map_err(|_| QueryError::Timeout)?,
    }
}

async fn query_tcp(server: SocketAddr) -> Result<Sample, QueryError> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(server)
        .await
        .map_err(QueryError::Io)?;
    let _ = stream.set_nodelay(true);
    let t1 = NtpTime::now();
    let request = Packet {
        mode: MODE_CLIENT,
        transmit: t1,
        ..Packet::default()
    };
    stream
        .write_all(&request.encode())
        .await
        .map_err(QueryError::Io)?;
    let mut buf = [0u8; PACKET_LEN];
    stream.read_exact(&mut buf).await.map_err(QueryError::Io)?;
    let t4 = NtpTime::now();
    let answer = Packet::decode(&buf).ok_or(QueryError::Refused("short answer"))?;
    sample(t1, &answer, t4)
}

/// Serves the node's clock over TCP: one 48-byte request and answer per connection, then close.
pub(crate) async fn serve_tcp(
    listener: tokio::net::TcpListener,
    clock: impl Fn() -> ServedClock + Send + Sync + 'static,
    now: impl Fn() -> NtpTime + Send + Sync + 'static,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let shared = std::sync::Arc::new((clock, now));
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let shared = std::sync::Arc::clone(&shared);
        tokio::spawn(async move {
            let _ = stream.set_nodelay(true);
            let mut buf = [0u8; PACKET_LEN];
            let read = tokio::time::timeout(QUERY_TIMEOUT, stream.read_exact(&mut buf)).await;
            if !matches!(read, Ok(Ok(_))) {
                return;
            }
            let (clock, now) = &*shared;
            let received = now();
            if let Some(reply) = answer(&buf, received, clock(), now()) {
                let _ = stream.write_all(&reply).await;
            }
        });
    }
}

/// Asks `server` for the time once over UDP.
pub(crate) async fn query(server: SocketAddr) -> Result<Sample, QueryError> {
    let bind: SocketAddr = if server.is_ipv4() {
        ([0, 0, 0, 0], 0).into()
    } else {
        (std::net::Ipv6Addr::UNSPECIFIED, 0).into()
    };
    let socket = UdpSocket::bind(bind).await.map_err(QueryError::Io)?;
    socket.connect(server).await.map_err(QueryError::Io)?;
    let t1 = NtpTime::now();
    let request = Packet {
        mode: MODE_CLIENT,
        transmit: t1,
        ..Packet::default()
    };
    socket
        .send(&request.encode())
        .await
        .map_err(QueryError::Io)?;
    let mut buf = [0u8; 64];
    let len = tokio::time::timeout(QUERY_TIMEOUT, socket.recv(&mut buf))
        .await
        .map_err(|_| QueryError::Timeout)?
        .map_err(QueryError::Io)?;
    let t4 = NtpTime::now();
    let answer = Packet::decode(&buf[..len]).ok_or(QueryError::Refused("short answer"))?;
    sample(t1, &answer, t4)
}

/// What the node's time service says about its own clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ServedClock {
    /// Synchronized at this stratum (its source's plus one).
    Synced(u8),
    /// Not synchronized, but configured as the network's reference: served at stratum 10.
    Authority,
    /// Not synchronized: answers say so, and clients ignore them.
    Unsynced,
}

/// The answer to one request, or `None` for anything but a client request.
pub(crate) fn answer(
    request: &[u8],
    received: NtpTime,
    clock: ServedClock,
    transmit: NtpTime,
) -> Option<[u8; PACKET_LEN]> {
    let request = Packet::decode(request)?;
    if request.mode != MODE_CLIENT {
        return None;
    }
    let (leap, stratum, reference_id) = match clock {
        ServedClock::Synced(stratum) => (0, stratum.clamp(2, 15), *b"ORIN"),
        ServedClock::Authority => (0, 10, *b"LOCL"),
        ServedClock::Unsynced => (LEAP_ALARM, 16, *b"INIT"),
    };
    Some(
        Packet {
            leap,
            mode: MODE_SERVER,
            stratum,
            reference_id,
            origin: request.transmit,
            receive: received,
            transmit,
        }
        .encode(),
    )
}

/// Serves the node's clock on `socket` until the task is dropped. `now` is the clock served
/// (`NtpTime::now`; tests serve a clock that is off).
pub(crate) async fn serve(
    socket: UdpSocket,
    clock: impl Fn() -> ServedClock,
    now: impl Fn() -> NtpTime,
) {
    let mut buf = [0u8; 128];
    loop {
        let Ok((len, peer)) = socket.recv_from(&mut buf).await else {
            continue;
        };
        let received = now();
        if let Some(reply) = answer(&buf[..len], received, clock(), now()) {
            let _ = socket.send_to(&reply, peer).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ntp_time_round_trips_through_unix_nanoseconds() {
        let ns = 1_762_500_000_123_456_789i128;
        let back = NtpTime::from_unix_ns(ns).to_unix_ns();
        assert!((back - ns).abs() < 2, "{back} vs {ns}");
    }

    #[test]
    fn a_served_answer_gives_the_clients_offset() {
        // The server's clock is 2 h ahead; the network takes 10 ms each way.
        let ahead = 7_200_000_000_000i128;
        let t1 = NtpTime::from_unix_ns(1_000_000_000_000);
        let request = Packet {
            mode: MODE_CLIENT,
            transmit: t1,
            ..Packet::default()
        }
        .encode();
        let t2 = NtpTime::from_unix_ns(1_000_010_000_000 + ahead);
        let t3 = NtpTime::from_unix_ns(1_000_011_000_000 + ahead);
        let reply = answer(&request, t2, ServedClock::Authority, t3).expect("answer");
        let t4 = NtpTime::from_unix_ns(1_000_021_000_000);
        let got = sample(t1, &Packet::decode(&reply).expect("decode"), t4).expect("sample");
        assert!((i128::from(got.offset_ns) - ahead).abs() < 10, "{got:?}");
        assert!((got.delay_ns - 20_000_000).abs() < 10, "{got:?}");
        assert_eq!(got.stratum, 10);
    }

    #[test]
    fn unsynchronized_and_foreign_answers_are_refused() {
        let t1 = NtpTime::from_unix_ns(5_000_000_000);
        let request = Packet {
            mode: MODE_CLIENT,
            transmit: t1,
            ..Packet::default()
        }
        .encode();
        let unsynced = answer(&request, t1, ServedClock::Unsynced, t1).expect("answer");
        let unsynced = Packet::decode(&unsynced).expect("decode");
        assert!(matches!(
            sample(t1, &unsynced, t1),
            Err(QueryError::Refused(_))
        ));
        let other = NtpTime::from_unix_ns(6_000_000_000);
        let synced = answer(&request, t1, ServedClock::Synced(3), t1).expect("answer");
        let synced = Packet::decode(&synced).expect("decode");
        assert!(matches!(
            sample(other, &synced, t1),
            Err(QueryError::Refused(_))
        ));
        // A server answer is not answered.
        assert!(answer(&synced.encode(), t1, ServedClock::Authority, t1).is_none());
    }
}
