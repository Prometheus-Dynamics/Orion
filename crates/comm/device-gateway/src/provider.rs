//! The gateway's own session: a provider in memory that writes the devices' records.

use std::time::Duration;

use orion_msg_records::{
    Offered, Put, RecordClass, RecordEntry, ResourceDescription, ResourceRecord, ResourceState,
};
use orion_msg_session::{Hello, Ping, Roles, Welcome};
use orion_server::{Comm, FdSet, Principal, mem};
use orion_types::{HlcTimestamp, NodeName, ResourceName};
use orion_wire::body::encode_fields;
use orion_wire::{Body, Bytes, Decode, List, encode_body, frame, kind};
use tokio::time::{Instant, sleep_until};

use crate::{DeviceGateway, Offers};

/// The largest frame either way.
const MAX_FRAME: usize = 64 * 1024;

/// How long to wait before writing again what node logic did not take.
const RETRY: Duration = Duration::from_secs(1);

pub(crate) fn start(comm: &Comm, node: NodeName, gateway: DeviceGateway) {
    let principal = Principal::Local {
        uid: rustix::process::getuid().as_raw(),
        gid: rustix::process::getgid().as_raw(),
    };
    let (server, client) = mem::pair(principal, Roles::PROVIDER, 4 * MAX_FRAME);
    comm.attach(server);
    tokio::task::spawn_local(async move {
        if let Err(error) = run(client, node, gateway).await {
            tracing::warn!(%error, "the device gateway's session ended; devices are not listed");
        }
    });
}

struct Session {
    connection: mem::MemConnection,
    seq: u16,
    out: Vec<u8>,
}

impl Session {
    async fn send<B: Body>(&mut self, body: &B) -> std::io::Result<u16> {
        self.seq = self.seq.wrapping_add(1);
        let len = encode_body(body, self.seq, &mut self.out)
            .map_err(|_| std::io::Error::other("a record too large for a frame"))?;
        self.connection.send(&self.out[..len], &[]).await?;
        Ok(self.seq)
    }
}

async fn run(
    connection: mem::MemConnection,
    node: NodeName,
    gateway: DeviceGateway,
) -> std::io::Result<()> {
    let mut session = Session {
        connection,
        seq: 0,
        out: vec![0; MAX_FRAME],
    };
    let mut buf = vec![0_u8; MAX_FRAME];
    let mut fds = FdSet::default();
    session
        .send(&Hello {
            name: "device-gateway",
            roles: Roles::PROVIDER,
            max_frame: MAX_FRAME as u32,
            revision: frame::INTERFACE_REVISION,
            level: frame::INTERFACE_LEVEL,
        })
        .await?;
    let heartbeat = loop {
        let Some(len) = session.connection.recv(&mut buf, &mut fds).await? else {
            return Err(std::io::Error::other("closed before welcome"));
        };
        let view = frame::decode(&buf[..len]).map_err(|_| std::io::Error::other("a bad frame"))?;
        if view.kind() == kind::WELCOME {
            let welcome = Welcome::decode(view.payload())
                .map_err(|_| std::io::Error::other("a bad welcome"))?;
            if welcome.revision != frame::INTERFACE_REVISION {
                return Err(std::io::Error::other(format!(
                    "the comm core speaks wire revision {} and the gateway {}",
                    welcome.revision,
                    frame::INTERFACE_REVISION
                )));
            }
            break Duration::from_millis(u64::from(welcome.heartbeat_ms.max(1)));
        }
    };
    let started = Instant::now();
    let mut next_ping = started + heartbeat;
    // Writes that went out and are not answered yet: their `seq` and what they said.
    let mut waiting: Vec<(u16, String, Offers, ResourceState)> = Vec::new();
    let mut retry_at: Option<Instant> = None;
    loop {
        let changed = gateway.state.borrow().changed();
        let due = retry_at.is_none_or(|at| Instant::now() >= at);
        if due {
            retry_at = None;
            for (name, offers, state) in changed {
                if waiting.iter().any(|(_, waiting, ..)| *waiting == name) {
                    continue;
                }
                match write(&mut session, &node, &name, &offers, state).await {
                    Ok(seq) => waiting.push((seq, name, offers, state)),
                    Err(error) => tracing::warn!(device = %name, %error, "a device's record"),
                }
            }
        }
        let wake = retry_at.map_or(next_ping, |at| at.min(next_ping));
        tokio::select! {
            () = gateway.changed.notified() => {}
            () = sleep_until(wake) => {
                if Instant::now() >= next_ping {
                    let now_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(0);
                    session.send(&Ping { now_ms }).await?;
                    next_ping = Instant::now() + heartbeat;
                }
            }
            received = session.connection.recv(&mut buf, &mut fds) => {
                let Some(len) = received? else {
                    return Err(std::io::Error::other("the comm core closed the session"));
                };
                let Ok(view) = frame::decode(&buf[..len]) else { continue };
                let answered = match view.kind() {
                    kind::CHANGED => Some((answer_seq(view.payload()), true)),
                    kind::REFUSAL => Some((answer_seq(view.payload()), false)),
                    _ => None,
                };
                if let Some((Some(seq), taken)) = answered
                    && let Some(at) = waiting.iter().position(|(sent, ..)| *sent == seq)
                {
                    let (_, name, offers, state) = waiting.swap_remove(at);
                    if taken {
                        gateway.state.borrow_mut().written(&name, offers, state);
                    } else {
                        tracing::debug!(device = %name, "a device's record was not taken; again soon");
                        retry_at = Some(Instant::now() + RETRY);
                    }
                }
            }
        }
    }
}

/// The `request_seq` an answer starts with.
fn answer_seq(payload: &[u8]) -> Option<u16> {
    orion_wire::body::take::<u16>(payload)
        .ok()
        .map(|(seq, _)| seq)
}

/// Writes device `name`'s resource record and returns the frame's `seq`.
async fn write(
    session: &mut Session,
    node: &NodeName,
    name: &str,
    offers: &Offers,
    state: ResourceState,
) -> std::io::Result<u16> {
    let invalid = |_| std::io::Error::other("not a resource name");
    let too_large = |_| std::io::Error::other("too much for a record");
    let resource: ResourceName = name.parse().map_err(invalid)?;
    let topics: Vec<Offered<'_>> = offers.topics.iter().map(|name| Offered { name }).collect();
    let actions: Vec<Offered<'_>> = offers.actions.iter().map(|name| Offered { name }).collect();
    let description = ResourceDescription {
        topics: List::new(&topics),
        actions: List::new(&actions),
        labels: List::default(),
    };
    let mut attributes = vec![0_u8; MAX_FRAME / 2];
    let len = encode_fields(&description, &mut attributes).map_err(too_large)?;
    let record = ResourceRecord {
        name: resource,
        kind: "device",
        node: node.clone(),
        state,
        attributes: Bytes(&attributes[..len]),
    };
    let mut value = vec![0_u8; MAX_FRAME / 2];
    let len = encode_fields(&record, &mut value).map_err(too_large)?;
    let entries = [RecordEntry {
        class: RecordClass::Resource,
        key: name,
        stamp: HlcTimestamp::ZERO,
        deleted: false,
        value: Bytes(&value[..len]),
    }];
    session
        .send(&Put {
            records: List::new(&entries),
        })
        .await
}
