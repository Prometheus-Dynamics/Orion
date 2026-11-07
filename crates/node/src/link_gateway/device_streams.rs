//! Streams of the devices on a serial or CAN link (revision 2): each device that declares one is a
//! publisher of its own (`link:<link>/<device>`, as a Unix session's), registered on its first
//! declaration and removed when its session ends. The node's demand for a device's streams is
//! queued here and sent by the link task, which owns the sessions.

use crate::{NodeApp, NodeError};
use orion::control_plane::{ClientEventKind, SampleChunk, SeriesDecl, StreamRef};
use orion::transport::ipc::LocalAddress;
use orion_link::Transport;
use orion_link::host::{ActionSendError, HostSession};
use orion_link::message::ErrorCode;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::debug;

/// Node events taken per device and drain.
const EVENTS_PER_DRAIN: usize = 32;
/// Frames waiting for the devices' sessions, past which the oldest demand is dropped (a newer
/// demand for the same stream replaces it anyway).
const OUT_LIMIT: usize = 64;

/// What the link task sends a device for its streams.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum StreamOut {
    /// A `StreamDemand` for a stream the device declared.
    Demand {
        id: String,
        subscribers: u32,
        series: Vec<u16>,
    },
    /// A refused declaration.
    Refuse {
        req: u32,
        code: ErrorCode,
        reason: String,
    },
}

struct Publisher {
    source: LocalAddress,
    declared: BTreeMap<String, StreamRef>,
}

/// The stream publishers of one link's devices.
pub(super) struct DeviceStreams {
    link: String,
    wake: Arc<Notify>,
    devices: BTreeMap<String, Publisher>,
    out: VecDeque<(String, StreamOut)>,
}

impl DeviceStreams {
    pub(super) fn new(link: &str) -> Self {
        Self {
            link: link.to_owned(),
            wake: Arc::new(Notify::new()),
            devices: BTreeMap::new(),
            out: VecDeque::new(),
        }
    }

    /// Notified when the node has events for one of the link's publishers.
    pub(super) fn wake(&self) -> Arc<Notify> {
        self.wake.clone()
    }

    /// A device's `StreamDeclare`: accepted (the node's first demand answers it) or refused.
    pub(super) fn declare(
        &mut self,
        app: &NodeApp,
        device: &str,
        req: u32,
        id: String,
        series: Vec<SeriesDecl>,
    ) {
        let publisher = self.devices.entry(device.to_owned()).or_insert_with(|| {
            let source = LocalAddress::new(format!("link:{}/{device}", self.link));
            app.register_link_publisher(&source, source.as_str(), self.wake.clone());
            Publisher {
                source,
                declared: BTreeMap::new(),
            }
        });
        match app.declare_link_stream(&publisher.source, &self.link, device, id.clone(), series) {
            Ok(stream) => {
                publisher.declared.insert(id, stream);
            }
            Err(error) => {
                debug!(link = %self.link, device, id, %error, "device stream declaration refused");
                let code = match &error {
                    NodeError::Status(_) => ErrorCode::INVALID,
                    other => super::unix_client::code_for(other),
                };
                self.push(
                    device,
                    StreamOut::Refuse {
                        req,
                        code,
                        reason: error.to_string(),
                    },
                );
            }
        }
    }

    /// A device's `StreamPublish`: dropped (and logged) when refused.
    pub(super) fn publish(
        &mut self,
        app: &NodeApp,
        device: &str,
        id: &str,
        chunks: Vec<SampleChunk>,
    ) {
        let Some((publisher, stream)) = self
            .devices
            .get(device)
            .and_then(|publisher| Some((publisher, publisher.declared.get(id)?)))
        else {
            debug!(link = %self.link, device, id, "batch for a stream the device did not declare");
            return;
        };
        if let Err(error) = app.publish_link_stream(&publisher.source, stream.clone(), chunks) {
            debug!(link = %self.link, device, id, %error, "device stream batch dropped");
        }
    }

    /// The device's session ended: its streams lose their publisher.
    pub(super) fn lost(&mut self, app: &NodeApp, device: &str) {
        if let Some(publisher) = self.devices.remove(device) {
            app.remove_link_client(&publisher.source);
        }
        self.out.retain(|(to, _)| to != device);
    }

    /// The link closed.
    pub(super) fn close(&mut self, app: &NodeApp) {
        for publisher in std::mem::take(&mut self.devices).into_values() {
            app.remove_link_client(&publisher.source);
        }
        self.out.clear();
    }

    /// Takes the node's demand for the devices' streams, then sends what is queued through `send`
    /// (the link's session of that device). A congested session keeps the rest for the next call;
    /// a device without a session loses what was queued for it.
    pub(super) fn pump(
        &mut self,
        app: &NodeApp,
        mut send: impl FnMut(&str, &StreamOut) -> Result<(), ActionSendError>,
    ) {
        let mut demands = Vec::new();
        for (device, publisher) in &self.devices {
            for event in app.take_link_client_events(&publisher.source, EVENTS_PER_DRAIN) {
                if let ClientEventKind::StreamDemand(demand) = event.event
                    && publisher.declared.get(&demand.stream.id) == Some(&demand.stream)
                {
                    demands.push((
                        device.clone(),
                        StreamOut::Demand {
                            id: demand.stream.id,
                            subscribers: demand.subscribers,
                            series: demand.series,
                        },
                    ));
                }
            }
        }
        for (device, out) in demands {
            self.push(&device, out);
        }
        while let Some((device, out)) = self.out.front() {
            match send(device, out) {
                Ok(()) => {}
                Err(ActionSendError::Congested) => return,
                Err(error) => {
                    debug!(link = %self.link, device, ?error, "device stream frame not sent");
                }
            }
            self.out.pop_front();
        }
    }

    fn push(&mut self, device: &str, out: StreamOut) {
        // A newer demand for the same stream replaces one not sent yet.
        if let StreamOut::Demand { id, .. } = &out {
            self.out.retain(|(to, queued)| {
                !(to == device
                    && matches!(queued, StreamOut::Demand { id: other, .. } if other == id))
            });
        }
        if self.out.len() >= OUT_LIMIT {
            self.out.pop_front();
        }
        self.out.push_back((device.to_owned(), out));
    }
}

/// Queues `out` on a device's session.
pub(super) fn send<T: Transport>(
    session: &mut HostSession<T>,
    out: &StreamOut,
) -> Result<(), ActionSendError> {
    match out {
        StreamOut::Demand {
            id,
            subscribers,
            series,
        } => session.send_stream_demand(id, *subscribers, series),
        StreamOut::Refuse { req, code, reason } => session.send_request_error(*req, *code, reason),
    }
}
