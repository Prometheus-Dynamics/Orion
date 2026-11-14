//! A store with resources over it, written as node logic writes.

#![allow(dead_code)]

pub use orion_msg_records::{
    Label, LeaseRecord, Offered, RecordClass, RecordEntry, ResourceDescription, ResourceRecord,
    ResourceState,
};
pub use orion_resources::{Caller, Resources, find, holder};
pub use orion_store::{Store, StoreConfig};
pub use orion_types::{Error, HlcTimestamp, NodeName, ResourceName};
pub use orion_wire::body::encode_fields;
pub use orion_wire::{Bytes, List};

pub const CAMERA_DRIVER: u32 = 10;
pub const OTHER: u32 = 20;
pub const NOW: u64 = 1_700_000_000_000;

pub fn node() -> NodeName {
    "board".parse().expect("name")
}

pub fn name(text: &str) -> ResourceName {
    text.parse().expect("name")
}

/// A camera's record, as a provider writes it.
pub fn camera(resource: &str, on: &str, state: ResourceState) -> Vec<u8> {
    let labels = [Label {
        key: "lens",
        value: "wide",
    }];
    let topics = [Offered {
        name: "cam.front.frames",
    }];
    let actions = [Offered {
        name: "cam.front.focus",
    }];
    let mut attributes = [0_u8; 256];
    let used = encode_fields(
        &ResourceDescription {
            topics: List::new(&topics),
            actions: List::new(&actions),
            labels: List::new(&labels),
        },
        &mut attributes,
    )
    .expect("encode");
    let mut value = vec![0_u8; 512];
    let len = encode_fields(
        &ResourceRecord {
            name: name(resource),
            kind: "camera",
            node: on.parse().expect("name"),
            state,
            attributes: Bytes(&attributes[..used]),
        },
        &mut value,
    )
    .expect("encode");
    value.truncate(len);
    value
}

pub fn entry<'a>(
    class: RecordClass,
    key: &'a str,
    value: &'a [u8],
    deleted: bool,
) -> RecordEntry<'a> {
    RecordEntry {
        class,
        key,
        stamp: HlcTimestamp::ZERO,
        deleted,
        value: Bytes(value),
    }
}

pub struct Bench {
    pub store: Store,
    pub resources: Resources,
    /// Node logic's clock.
    pub now: u64,
    /// Runs under way: the session that asked, and the action.
    pub running: Vec<(u32, String)>,
    /// What the last refusal said besides its code.
    pub said: Option<String>,
}

impl Bench {
    pub fn new() -> Self {
        Self {
            store: Store::new(node(), StoreConfig::default()),
            resources: Resources::new(node()),
            now: 0,
            running: Vec::new(),
            said: None,
        }
    }

    /// Writes as node logic does: checked first, then stored, then noted.
    /// As a program of user 1000.
    pub fn write(&mut self, session: u32, entry: &RecordEntry<'_>) -> Result<(), Error> {
        self.write_as(
            &Caller {
                session,
                principal: "local:1000",
                root: false,
            },
            entry,
        )
    }

    /// Writes as `caller`.
    pub fn write_as(&mut self, caller: &Caller<'_>, entry: &RecordEntry<'_>) -> Result<(), Error> {
        let session = caller.session;
        let running = &self.running;
        let in_flight = |asker: u32, names: &[String]| {
            running
                .iter()
                .any(|(by, action)| *by == asker && names.contains(action))
        };
        let taken = self
            .resources
            .check(&self.store, caller, entry, (NOW, self.now), &in_flight)
            .map_err(|refused| {
                self.said = refused.words;
                refused.error
            })?;
        if entry.deleted {
            self.store.delete(entry.class, entry.key, NOW);
        } else {
            self.store.put(entry.class, entry.key, entry.value.0, NOW);
        }
        self.resources.wrote(&mut self.store, session, entry, NOW);
        if let Some(taken) = taken {
            self.resources.taken(taken, session);
        }
        Ok(())
    }

    pub fn provide(
        &mut self,
        session: u32,
        resource: &str,
        state: ResourceState,
    ) -> Result<(), Error> {
        let value = camera(resource, "board", state);
        self.write(
            session,
            &entry(RecordClass::Resource, resource, &value, false),
        )
    }

    /// With a camera, ready.
    pub fn with_camera() -> Self {
        let mut bench = Self::new();
        bench
            .provide(CAMERA_DRIVER, "cam.front", ResourceState::Ready)
            .expect("provide");
        bench
    }

    pub fn state(&self, resource: &str) -> Option<ResourceState> {
        find(&self.store, resource, NOW).map(|found| found.state)
    }
}
