//! Resources over a store: one provider each, lost when it goes, held by one at a time.

use orion_msg_records::{
    Label, LeaseRecord, Offered, RecordClass, RecordEntry, ResourceDescription, ResourceRecord,
    ResourceState,
};
use orion_resources::{Resources, bind, find, holder, list, release, release_all};
use orion_store::{Store, StoreConfig};
use orion_types::{Error, HlcTimestamp, NodeName, ResourceName};
use orion_wire::body::encode_fields;
use orion_wire::{Bytes, List};

const CAMERA_DRIVER: u32 = 10;
const OTHER: u32 = 20;
const NOW: u64 = 1_700_000_000_000;

fn node() -> NodeName {
    "board".parse().expect("name")
}

fn name(text: &str) -> ResourceName {
    text.parse().expect("name")
}

/// A camera's record, as a provider writes it.
fn camera(resource: &str, on: &str, state: ResourceState) -> Vec<u8> {
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

fn entry<'a>(class: RecordClass, key: &'a str, value: &'a [u8], deleted: bool) -> RecordEntry<'a> {
    RecordEntry {
        class,
        key,
        stamp: HlcTimestamp::ZERO,
        deleted,
        value: Bytes(value),
    }
}

struct Bench {
    store: Store,
    resources: Resources,
}

impl Bench {
    fn new() -> Self {
        Self {
            store: Store::new(node(), StoreConfig::default()),
            resources: Resources::new(node()),
        }
    }

    /// Writes as node logic does: checked first, then stored, then noted.
    fn write(&mut self, session: u32, entry: &RecordEntry<'_>) -> Result<(), Error> {
        self.resources.check(&self.store, session, entry, NOW)?;
        if entry.deleted {
            self.store.delete(entry.class, entry.key, NOW);
        } else {
            self.store.put(entry.class, entry.key, entry.value.0, NOW);
        }
        self.resources.wrote(&mut self.store, session, entry, NOW);
        Ok(())
    }

    fn provide(&mut self, session: u32, resource: &str, state: ResourceState) -> Result<(), Error> {
        let value = camera(resource, "board", state);
        self.write(
            session,
            &entry(RecordClass::Resource, resource, &value, false),
        )
    }

    /// With a camera, ready.
    fn with_camera() -> Self {
        let mut bench = Self::new();
        bench
            .provide(CAMERA_DRIVER, "cam.front", ResourceState::Ready)
            .expect("provide");
        bench
    }

    fn state(&self, resource: &str) -> Option<ResourceState> {
        find(&self.store, resource, NOW).map(|found| found.state)
    }
}

#[test]
fn a_device_from_a_provider_is_listed_with_what_it_offers() {
    let bench = Bench::with_camera();
    let found = list(&bench.store, "", NOW);
    assert_eq!(found.len(), 1);
    let camera = &found[0];
    assert_eq!(
        (camera.name.as_str(), camera.kind.as_str()),
        ("cam.front", "camera")
    );
    assert_eq!(camera.node, node());
    assert_eq!(camera.state, ResourceState::Ready);
    assert_eq!(camera.labels, [("lens".to_owned(), "wide".to_owned())]);
    assert_eq!(camera.topics, ["cam.front.frames"]);
    assert_eq!(camera.actions, ["cam.front.focus"]);
    assert_eq!(camera.holder, None);
    assert!(list(&bench.store, "fan.", NOW).is_empty());
    assert_eq!(list(&bench.store, "cam.", NOW).len(), 1);
}

#[test]
fn a_resource_is_bound_to_one_holder_at_a_time() {
    let mut bench = Bench::with_camera();
    let camera = name("cam.front");
    bind(&mut bench.store, &camera, "workload:detector", 0, NOW).expect("bind");
    assert_eq!(
        find(&bench.store, "cam.front", NOW)
            .expect("camera")
            .holder
            .as_deref(),
        Some("workload:detector")
    );
    // Another is refused. The holder may bind again.
    assert_eq!(
        bind(&mut bench.store, &camera, "workload:recorder", 0, NOW),
        Err(Error::Busy)
    );
    bind(&mut bench.store, &camera, "workload:detector", 0, NOW).expect("again");
    // Only the holder releases it.
    assert!(!release(
        &mut bench.store,
        "cam.front",
        "workload:recorder",
        NOW
    ));
    assert!(release(
        &mut bench.store,
        "cam.front",
        "workload:detector",
        NOW
    ));
    assert_eq!(holder(&bench.store, "cam.front", NOW), None);
    bind(&mut bench.store, &camera, "workload:recorder", 0, NOW).expect("free now");
}

#[test]
fn a_lease_that_has_ended_holds_nothing() {
    let mut bench = Bench::with_camera();
    let camera = name("cam.front");
    bind(
        &mut bench.store,
        &camera,
        "workload:detector",
        NOW + 1_000,
        NOW,
    )
    .expect("bind");
    assert_eq!(
        bind(&mut bench.store, &camera, "workload:recorder", 0, NOW + 999),
        Err(Error::Busy)
    );
    assert_eq!(holder(&bench.store, "cam.front", NOW + 1_000), None);
    bind(
        &mut bench.store,
        &camera,
        "workload:recorder",
        0,
        NOW + 1_000,
    )
    .expect("it ended");
    // Renewing moves the end.
    bind(
        &mut bench.store,
        &camera,
        "workload:recorder",
        NOW + 5_000,
        NOW + 1_500,
    )
    .expect("renew");
    assert_eq!(holder(&bench.store, "cam.front", NOW + 6_000), None);
}

#[test]
fn what_is_not_there_or_not_ready_is_not_bound() {
    let mut bench = Bench::new();
    assert_eq!(
        bind(&mut bench.store, &name("cam.front"), "w", 0, NOW),
        Err(Error::NotFound)
    );
    bench
        .provide(CAMERA_DRIVER, "cam.front", ResourceState::Pending)
        .expect("provide");
    assert_eq!(
        bind(&mut bench.store, &name("cam.front"), "w", 0, NOW),
        Err(Error::Unavailable)
    );
    bench
        .provide(CAMERA_DRIVER, "cam.front", ResourceState::Failed)
        .expect("provide");
    assert_eq!(
        bind(&mut bench.store, &name("cam.front"), "w", 0, NOW),
        Err(Error::Unavailable)
    );
}

#[test]
fn a_resource_has_one_provider() {
    let mut bench = Bench::with_camera();
    // Another program may neither write it nor take it away.
    assert_eq!(
        bench.provide(OTHER, "cam.front", ResourceState::Failed),
        Err(Error::Denied)
    );
    assert_eq!(
        bench.write(OTHER, &entry(RecordClass::Resource, "cam.front", &[], true)),
        Err(Error::Denied)
    );
    assert_eq!(bench.state("cam.front"), Some(ResourceState::Ready));
    // Its provider may do both.
    bench
        .provide(CAMERA_DRIVER, "cam.front", ResourceState::Failed)
        .expect("its own");
    assert_eq!(bench.state("cam.front"), Some(ResourceState::Failed));
    // And another program provides something else.
    bench
        .provide(OTHER, "cam.rear", ResourceState::Ready)
        .expect("another resource");
    assert_eq!(bench.resources.provided(), 2);
}

#[test]
fn a_record_that_is_not_a_resource_of_this_node_is_refused() {
    let mut bench = Bench::new();
    // Not a resource record at all.
    assert_eq!(
        bench.write(
            CAMERA_DRIVER,
            &entry(RecordClass::Resource, "cam.front", &[0xFF; 3], false)
        ),
        Err(Error::Malformed)
    );
    // Under another name than its own.
    let value = camera("cam.front", "board", ResourceState::Ready);
    assert_eq!(
        bench.write(
            CAMERA_DRIVER,
            &entry(RecordClass::Resource, "cam.rear", &value, false)
        ),
        Err(Error::Malformed)
    );
    // On another node: a program says what is on its own board.
    let value = camera("cam.front", "elsewhere", ResourceState::Ready);
    assert_eq!(
        bench.write(
            CAMERA_DRIVER,
            &entry(RecordClass::Resource, "cam.front", &value, false)
        ),
        Err(Error::Denied)
    );
    // Other classes are not looked at.
    bench
        .write(
            CAMERA_DRIVER,
            &entry(RecordClass::Node, "board", &[0xFF; 3], false),
        )
        .expect("not this crate's");
    assert!(list(&bench.store, "", NOW).is_empty());
}

#[test]
fn when_its_provider_goes_a_resource_is_lost_and_comes_back_with_it() {
    let mut bench = Bench::with_camera();
    bench
        .provide(OTHER, "cam.rear", ResourceState::Ready)
        .expect("provide");
    bind(
        &mut bench.store,
        &name("cam.front"),
        "workload:detector",
        0,
        NOW,
    )
    .expect("bind");

    assert_eq!(
        bench.resources.gone(&mut bench.store, CAMERA_DRIVER, NOW),
        1
    );
    let lost = find(&bench.store, "cam.front", NOW).expect("it is still listed");
    assert_eq!(lost.state, ResourceState::Lost);
    // Everything else about it is as its provider said.
    assert_eq!(lost.topics, ["cam.front.frames"]);
    assert_eq!(lost.labels.len(), 1);
    // Its holder still holds it, and nobody else can take it.
    assert_eq!(lost.holder.as_deref(), Some("workload:detector"));
    bind(
        &mut bench.store,
        &name("cam.front"),
        "workload:detector",
        0,
        NOW,
    )
    .expect("keeps it");
    assert_eq!(
        bind(
            &mut bench.store,
            &name("cam.front"),
            "workload:recorder",
            0,
            NOW
        ),
        Err(Error::Busy)
    );
    // The other provider's resource is untouched.
    assert_eq!(bench.state("cam.rear"), Some(ResourceState::Ready));
    // Gone twice is nothing.
    assert_eq!(
        bench.resources.gone(&mut bench.store, CAMERA_DRIVER, NOW),
        0
    );

    // The provider is back, in a new session, and says it is ready.
    bench
        .provide(30, "cam.front", ResourceState::Ready)
        .expect("back");
    assert_eq!(bench.state("cam.front"), Some(ResourceState::Ready));
}

#[test]
fn a_lost_resource_that_nobody_holds_cannot_be_taken() {
    let mut bench = Bench::with_camera();
    bench.resources.gone(&mut bench.store, CAMERA_DRIVER, NOW);
    assert_eq!(
        bind(&mut bench.store, &name("cam.front"), "w", 0, NOW),
        Err(Error::Unavailable)
    );
}

#[test]
fn a_resource_taken_away_takes_its_lease_with_it() {
    let mut bench = Bench::with_camera();
    bind(
        &mut bench.store,
        &name("cam.front"),
        "workload:detector",
        0,
        NOW,
    )
    .expect("bind");
    bench
        .write(
            CAMERA_DRIVER,
            &entry(RecordClass::Resource, "cam.front", &[], true),
        )
        .expect("withdraw");
    assert!(find(&bench.store, "cam.front", NOW).is_none());
    assert_eq!(holder(&bench.store, "cam.front", NOW), None);
    assert_eq!(bench.resources.provided(), 0);
    // The name is free for whoever provides it next.
    bench
        .provide(OTHER, "cam.front", ResourceState::Ready)
        .expect("another provider");
}

#[test]
fn a_lease_written_as_a_record_follows_the_same_rules() {
    let mut bench = Bench::with_camera();
    let lease = |holder: &str, resource: &str| {
        let mut value = vec![0_u8; 128];
        let len = encode_fields(
            &LeaseRecord {
                resource: name(resource),
                holder,
                expires_ms: 0,
            },
            &mut value,
        )
        .expect("encode");
        value.truncate(len);
        value
    };
    let first = lease("console", "cam.front");
    bench
        .write(
            OTHER,
            &entry(RecordClass::Lease, "cam.front", &first, false),
        )
        .expect("a free resource");
    assert_eq!(
        holder(&bench.store, "cam.front", NOW).as_deref(),
        Some("console")
    );
    let second = lease("someone-else", "cam.front");
    assert_eq!(
        bench.write(
            OTHER,
            &entry(RecordClass::Lease, "cam.front", &second, false)
        ),
        Err(Error::Busy)
    );
    // A lease for something that is not there, under the wrong name, or held by nobody.
    let nothing = lease("console", "cam.none");
    assert_eq!(
        bench.write(
            OTHER,
            &entry(RecordClass::Lease, "cam.none", &nothing, false)
        ),
        Err(Error::NotFound)
    );
    assert_eq!(
        bench.write(OTHER, &entry(RecordClass::Lease, "cam.rear", &first, false)),
        Err(Error::Malformed)
    );
    let nobody = lease("", "cam.front");
    assert_eq!(
        bench.write(
            OTHER,
            &entry(RecordClass::Lease, "cam.front", &nobody, false)
        ),
        Err(Error::Malformed)
    );
    // Deleting the lease releases it.
    bench
        .write(OTHER, &entry(RecordClass::Lease, "cam.front", &[], true))
        .expect("release");
    assert_eq!(holder(&bench.store, "cam.front", NOW), None);
}

#[test]
fn everything_a_holder_holds_is_released_at_once() {
    let mut bench = Bench::with_camera();
    bench
        .provide(CAMERA_DRIVER, "cam.rear", ResourceState::Ready)
        .expect("provide");
    bench
        .provide(CAMERA_DRIVER, "cam.side", ResourceState::Ready)
        .expect("provide");
    bind(
        &mut bench.store,
        &name("cam.front"),
        "workload:detector",
        0,
        NOW,
    )
    .expect("bind");
    bind(
        &mut bench.store,
        &name("cam.rear"),
        "workload:detector",
        0,
        NOW,
    )
    .expect("bind");
    bind(
        &mut bench.store,
        &name("cam.side"),
        "workload:recorder",
        0,
        NOW,
    )
    .expect("bind");
    assert_eq!(release_all(&mut bench.store, "workload:detector", NOW), 2);
    assert_eq!(holder(&bench.store, "cam.front", NOW), None);
    assert_eq!(
        holder(&bench.store, "cam.side", NOW).as_deref(),
        Some("workload:recorder")
    );
}

#[test]
fn a_comm_core_that_goes_loses_every_resource_here() {
    let mut bench = Bench::with_camera();
    bench
        .provide(OTHER, "cam.rear", ResourceState::Ready)
        .expect("provide");
    assert_eq!(bench.resources.forget_sessions(&mut bench.store, NOW), 2);
    assert_eq!(bench.state("cam.front"), Some(ResourceState::Lost));
    assert_eq!(bench.state("cam.rear"), Some(ResourceState::Lost));
    assert_eq!(bench.resources.provided(), 0);
}
