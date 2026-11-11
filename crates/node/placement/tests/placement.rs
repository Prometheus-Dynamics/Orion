//! `dev`'s placement tests, carried over: eligibility, the rendezvous choice and its
//! properties, who writes, hysteresis and the grace period, and binding across nodes.

use std::collections::{BTreeMap, BTreeSet};

use orion_placement::{
    Candidate, Coordinator, LABEL_COLOCATE, LABEL_RESOURCE, Needs, Offer, Reason, Step, View,
    choose_resource, holder_name, holder_parts, merge_labels, rendezvous_choice, rendezvous_score,
    stale_leases,
};
use orion_types::{NodeName, WorkloadKind, WorkloadName};
use orion_workloads::Workload;

fn name(text: &str) -> NodeName {
    text.parse().expect("node")
}

fn graph() -> WorkloadKind {
    "graph".parse().expect("kind")
}

fn labels(terms: &[&str]) -> BTreeMap<String, String> {
    terms
        .iter()
        .map(|term| match term.split_once('=') {
            Some((key, value)) => (key.to_owned(), value.to_owned()),
            None => ((*term).to_owned(), String::new()),
        })
        .collect()
}

fn node(terms: &[&str]) -> Candidate {
    Candidate {
        labels: labels(terms),
        live: true,
        schedulable: true,
        kinds: BTreeSet::from([graph()]),
    }
}

fn view(nodes: &[(&str, &[&str])]) -> View {
    View {
        nodes: nodes
            .iter()
            .map(|(id, terms)| (name(id), node(terms)))
            .collect(),
        resource_hosts: BTreeMap::new(),
    }
}

fn needs(id: &str, terms: &[&str]) -> Needs {
    Needs {
        name: id.parse().expect("workload"),
        kind: graph(),
        node: None,
        labels: labels(terms).into_iter().collect(),
        colocate: None,
        resources: Vec::new(),
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

#[test]
fn selector_filters_by_label_equality_and_existence() {
    let view = view(&[
        ("a", &["zone=north", "gpu"]),
        ("b", &["zone=south", "gpu"]),
        ("c", &["zone=north"]),
    ]);
    assert_eq!(
        view.eligible_nodes(&needs("w", &["zone=north"])),
        [name("a"), name("c")]
    );
    // A label with no value asks only that the label is there.
    assert_eq!(
        view.eligible_nodes(&needs("w", &["gpu"])),
        [name("a"), name("b")]
    );
    assert_eq!(
        view.eligible_nodes(&needs("w", &["zone=north", "gpu"])),
        [name("a")]
    );
    assert!(view.eligible_nodes(&needs("w", &["zone=west"])).is_empty());
    assert_eq!(view.choose(&needs("w", &["zone=west"])), None);
    assert_eq!(view.eligible_nodes(&needs("w", &[])).len(), 3);
}

#[test]
fn unreachable_unschedulable_and_unsupported_nodes_are_ineligible() {
    let mut view = view(&[("a", &[]), ("b", &[]), ("c", &[]), ("d", &[])]);
    view.nodes.get_mut(&name("a")).expect("a").live = false;
    view.nodes.get_mut(&name("b")).expect("b").schedulable = false;
    view.nodes.get_mut(&name("c")).expect("c").kinds.clear();
    let needs = needs("w", &[]);
    assert_eq!(view.eligible_nodes(&needs), [name("d")]);
    assert_eq!(view.choose(&needs), Some(name("d")));
    // A node nobody knows is not eligible.
    assert!(!view.eligible(&name("z"), &needs));
}

#[test]
fn colocation_follows_the_resource_host() {
    let mut view = view(&[("a", &[]), ("b", &[]), ("c", &[])]);
    view.resource_hosts.insert("cam.front".into(), name("b"));
    let mut beside = needs("w", &[]);
    beside.colocate = Some("cam.front".into());
    assert_eq!(view.eligible_nodes(&beside), [name("b")]);
    // The resource moves: so does where the workload may run.
    view.resource_hosts.insert("cam.front".into(), name("c"));
    assert_eq!(view.choose(&beside), Some(name("c")));
    // A resource that is nowhere: nowhere to run.
    beside.colocate = Some("cam.none".into());
    assert_eq!(view.choose(&beside), None);
}

#[test]
fn desired_labels_override_reported_labels_per_key() {
    let reported = labels(&["zone=north", "gpu", "rack=3"]);
    let desired = labels(&["zone=south", "camera=front"]);
    assert_eq!(
        merge_labels(&reported, &desired),
        labels(&["zone=south", "gpu", "rack=3", "camera=front"])
    );
}

#[test]
fn rendezvous_choice_is_deterministic_and_minimally_disruptive_property() {
    let mut rng = Rng(0x0510_7a11);
    for _ in 0..300 {
        let count = 1 + rng.below(12) as usize;
        let ids: BTreeSet<String> = (0..count)
            .map(|_| format!("node-{}", rng.below(40)))
            .collect();
        let zones = ["zone=a", "zone=b"];
        let nodes: Vec<(String, &str)> = ids
            .iter()
            .map(|id| (id.clone(), zones[rng.below(2) as usize]))
            .collect();
        let build = |order: &[(String, &str)]| View {
            nodes: order
                .iter()
                .map(|(id, label)| (name(id), node(&[label])))
                .collect(),
            resource_hosts: BTreeMap::new(),
        };
        let forward = build(&nodes);
        let mut reversed = nodes.clone();
        reversed.reverse();
        let backward = build(&reversed);
        for index in 0..8 {
            let terms: &[&str] = if index % 2 == 0 { &[] } else { &["zone=a"] };
            let workload = needs(&format!("workload.{}", rng.next()), terms);
            let chosen = forward.choose(&workload);
            // Same answer from every observer, whatever order it learned the nodes in.
            assert_eq!(chosen, backward.choose(&workload));
            // The answer is an eligible node, and exists whenever any node is eligible.
            let eligible = forward.eligible_nodes(&workload);
            assert_eq!(chosen.is_some(), !eligible.is_empty());
            if let Some(chosen) = chosen {
                assert!(eligible.contains(&chosen));
                // Removing a node that was not chosen never changes the choice.
                for other in eligible.iter().filter(|id| **id != chosen) {
                    let mut without = forward.clone();
                    without.nodes.remove(other);
                    assert_eq!(without.choose(&workload).as_ref(), Some(&chosen));
                }
                // Adding a node either keeps the choice or picks the new node.
                let mut with = forward.clone();
                let newcomer = name(&format!("node-new-{}", rng.next()));
                with.nodes.insert(newcomer.clone(), node(&["zone=a"]));
                let after = with.choose(&workload).expect("still eligible");
                assert!(after == chosen || after == newcomer);
            }
        }
    }
}

#[test]
fn the_hash_is_the_one_dev_used() {
    // Fixed across releases and platforms: nodes of different builds must agree.
    let scores = [
        rendezvous_score("workload.detector", "node-a"),
        rendezvous_score("workload.detector", "node-b"),
        rendezvous_score("workload.recorder", "node-a"),
    ];
    assert_eq!(scores, PINNED);
    // The key and the node are kept apart: ("ab", "c") is not ("a", "bc").
    assert_ne!(rendezvous_score("ab", "c"), rendezvous_score("a", "bc"));
    let nodes = [String::from("x"), String::from("y")];
    let chosen = rendezvous_choice("k", nodes.iter()).expect("one");
    assert!(
        rendezvous_score("k", chosen) >= rendezvous_score("k", "x")
            && rendezvous_score("k", chosen) >= rendezvous_score("k", "y")
    );
}

const PINNED: [u64; 3] = [
    0x514b_89e7_1889_19d6,
    0x319a_d056_cb49_faab,
    0x4b73_1357_f78a_e731,
];

/// Three nodes that all see each other.
fn three() -> View {
    view(&[("a", &[]), ("b", &[]), ("c", &[])])
}

#[test]
fn only_the_chosen_node_writes_and_running_workloads_do_not_flap() {
    let view = three();
    let workload = needs("workload.detector", &[]);
    let chosen = view.choose(&workload).expect("a node");
    // Every node looks; exactly the chosen one takes it.
    let mut takers = Vec::new();
    for local in ["a", "b", "c"] {
        let mut coordinator = Coordinator::new(name(local), 10_000);
        if coordinator.step(&view, &workload, None, 0) == Step::Take(Reason::Placed) {
            takers.push(name(local));
        }
    }
    assert_eq!(takers, std::slice::from_ref(&chosen));

    // Assigned and running: nobody does anything, at any time.
    for local in ["a", "b", "c"] {
        let mut coordinator = Coordinator::new(name(local), 10_000);
        for now in [0, 5_000, 1_000_000] {
            assert_eq!(
                coordinator.step(&view, &workload, Some(&chosen), now),
                Step::Nothing
            );
        }
    }

    // A node joins that would win it now. It stays where it is.
    let mut bigger = view.clone();
    let mut newcomer = None;
    for index in 0..200 {
        let candidate = name(&format!("n{index}"));
        bigger.nodes.insert(candidate.clone(), node(&[]));
        if bigger.choose(&workload) == Some(candidate.clone()) {
            newcomer = Some(candidate);
            break;
        }
        bigger.nodes.remove(&candidate);
    }
    let newcomer = newcomer.expect("some node wins it");
    let mut coordinator = Coordinator::new(newcomer, 10_000);
    assert_eq!(
        coordinator.step(&bigger, &workload, Some(&chosen), 1_000_000),
        Step::Nothing
    );
}

#[test]
fn failover_waits_for_the_grace_period_and_explicit_assignment_wins() {
    let mut view = three();
    let workload = needs("workload.detector", &[]);
    let first = view.choose(&workload).expect("a node");
    // The node that has it goes away.
    view.nodes.get_mut(&first).expect("first").live = false;
    let heir = view.choose(&workload).expect("another node");
    assert_ne!(heir, first);
    let other = ["a", "b", "c"]
        .iter()
        .map(|id| name(id))
        .find(|id| *id != first && *id != heir)
        .expect("the third");

    let mut on_heir = Coordinator::new(heir.clone(), 10_000);
    let mut on_other = Coordinator::new(other, 10_000);
    // Inside the grace period nothing moves.
    assert_eq!(
        on_heir.step(&view, &workload, Some(&first), 1_000),
        Step::Nothing
    );
    assert_eq!(on_heir.next_deadline(), Some(11_000));
    assert_eq!(
        on_heir.step(&view, &workload, Some(&first), 10_999),
        Step::Nothing
    );
    // After it, the heir takes it, and only the heir.
    assert_eq!(
        on_other.step(&view, &workload, Some(&first), 1_000),
        Step::Nothing
    );
    assert_eq!(
        on_other.step(&view, &workload, Some(&first), 11_000),
        Step::Nothing
    );
    assert_eq!(
        on_heir.step(&view, &workload, Some(&first), 11_000),
        Step::Take(Reason::Failover {
            from: first.clone()
        })
    );
    assert_eq!(on_heir.next_deadline(), None);

    // If the node is back within the grace period, the wait starts over the next time.
    let mut on_heir = Coordinator::new(heir.clone(), 10_000);
    assert_eq!(
        on_heir.step(&view, &workload, Some(&first), 0),
        Step::Nothing
    );
    view.nodes.get_mut(&first).expect("first").live = true;
    assert_eq!(
        on_heir.step(&view, &workload, Some(&first), 9_000),
        Step::Nothing
    );
    view.nodes.get_mut(&first).expect("first").live = false;
    assert_eq!(
        on_heir.step(&view, &workload, Some(&first), 12_000),
        Step::Nothing
    );
    assert_eq!(
        on_heir.step(&view, &workload, Some(&first), 22_000),
        Step::Take(Reason::Failover {
            from: first.clone()
        })
    );

    // A workload that names its node runs there and nowhere else, even while it is gone.
    let mut told = workload.clone();
    told.node = Some(first.clone());
    let mut on_heir = Coordinator::new(heir, 10_000);
    for now in [0, 50_000, 5_000_000] {
        assert_eq!(on_heir.step(&view, &told, Some(&first), now), Step::Nothing);
        assert_eq!(on_heir.step(&view, &told, None, now), Step::Nothing);
    }
    // The node it names takes it, once.
    let mut on_first = Coordinator::new(first.clone(), 10_000);
    assert_eq!(
        on_first.step(&view, &told, None, 0),
        Step::Take(Reason::Told)
    );
    assert_eq!(on_first.step(&view, &told, Some(&first), 0), Step::Nothing);
}

#[test]
fn a_workload_nobody_can_run_is_taken_by_nobody() {
    let view = three();
    let workload = needs("workload.detector", &["zone=nowhere"]);
    for local in ["a", "b", "c"] {
        let mut coordinator = Coordinator::new(name(local), 0);
        assert_eq!(coordinator.step(&view, &workload, None, 0), Step::Nothing);
        // Nor from a node that cannot run it any more, however long ago.
        assert_eq!(
            coordinator.step(&view, &workload, Some(&name("a")), 1_000_000),
            Step::Nothing
        );
    }
}

fn offer(resource: &str, on: &str, holder: Option<&str>) -> Offer {
    Offer {
        name: resource.to_owned(),
        node: name(on),
        ready: true,
        holder: holder.map(str::to_owned),
    }
}

#[test]
fn exclusive_remote_resource_admits_one_holder_and_owner_evicts_overflow() {
    let view = three();
    let detector: WorkloadName = "detector".parse().expect("name");
    let recorder: WorkloadName = "recorder".parse().expect("name");
    let mine = holder_name(&name("a"), &detector);
    assert_eq!(holder_parts(&mine), Some((name("a"), detector.clone())));
    assert_eq!(holder_parts("console"), None);

    // One camera, on another node, free: the workload on A is given it.
    let offers = [offer("cam.front", "b", None)];
    assert_eq!(
        choose_resource(&detector, &mine, &offers, &view).map(|offer| offer.name.as_str()),
        Some("cam.front")
    );
    // Held by the detector: the recorder is given nothing, and the detector keeps it.
    let offers = [offer("cam.front", "b", Some(&mine))];
    let theirs = holder_name(&name("c"), &recorder);
    assert_eq!(choose_resource(&recorder, &theirs, &offers, &view), None);
    assert_eq!(
        choose_resource(&detector, &mine, &offers, &view).map(|offer| offer.name.as_str()),
        Some("cam.front")
    );

    // The owner arbitrates: a lease whose workload is no longer assigned to the node that
    // holds it is dropped. Other nodes' resources are not this node's to judge.
    let offers = [
        offer("cam.front", "b", Some(&mine)),
        offer("cam.rear", "b", Some(&theirs)),
        offer("cam.side", "c", Some(&mine)),
        offer("cam.top", "b", Some("console")),
        offer("cam.free", "b", None),
    ];
    let assigned = |workload: &WorkloadName| -> Option<NodeName> {
        // The detector moved to C. The recorder is where its lease says.
        (*workload == detector)
            .then(|| name("c"))
            .or_else(|| (*workload == recorder).then(|| name("c")))
    };
    assert_eq!(stale_leases(&name("b"), &offers, assigned), ["cam.front"]);
    // A workload that is gone holds nothing.
    assert_eq!(
        stale_leases(&name("b"), &offers, |_| None),
        ["cam.front", "cam.rear"]
    );
}

#[test]
fn equal_resources_are_spread_and_every_node_gives_the_same_answer() {
    let mut view = three();
    let offers: Vec<Offer> = (0..4)
        .map(|index| offer(&format!("cam.{index}"), ["a", "b", "c"][index % 3], None))
        .collect();
    let mut given: BTreeMap<String, u32> = BTreeMap::new();
    for index in 0..400 {
        let workload: WorkloadName = format!("w{index}").parse().expect("name");
        let holder = holder_name(&name("a"), &workload);
        let chosen = choose_resource(&workload, &holder, &offers, &view).expect("one is free");
        // The same whatever order the offers are in.
        let mut reversed = offers.clone();
        reversed.reverse();
        assert_eq!(
            choose_resource(&workload, &holder, &reversed, &view).map(|offer| &offer.name),
            Some(&chosen.name)
        );
        *given.entry(chosen.name.clone()).or_default() += 1;
    }
    assert_eq!(given.len(), 4);
    assert!(
        given.values().all(|count| (60..=140).contains(count)),
        "{given:?}"
    );

    // One that is not ready, one that is held, one on a node that is gone: not given.
    let mut offers = offers;
    offers[0].ready = false;
    offers[1].holder = Some("b/other".into());
    view.nodes.get_mut(&name("c")).expect("c").live = false;
    let workload: WorkloadName = "w".parse().expect("name");
    let chosen = choose_resource(&workload, "a/w", &offers, &view).expect("one left");
    assert_eq!(chosen.name, "cam.3");
}

#[test]
fn what_a_workload_needs_is_read_from_what_was_put() {
    let mut workload = Workload::new("detector".parse().expect("name"), graph(), b"spec");
    workload.node = Some(name("left"));
    workload.labels = vec![
        ("zone".into(), "north".into()),
        (LABEL_RESOURCE.into(), "cam.front".into()),
        (LABEL_RESOURCE.into(), "light.ring".into()),
        (LABEL_COLOCATE.into(), "cam.front".into()),
        ("gpu".into(), String::new()),
    ];
    let needs = Needs::of(&workload);
    assert_eq!(needs.node, Some(name("left")));
    assert_eq!(
        needs.labels,
        [
            ("zone".to_owned(), "north".to_owned()),
            ("gpu".to_owned(), String::new())
        ]
    );
    assert_eq!(needs.resources, ["cam.front", "light.ring"]);
    assert_eq!(needs.colocate.as_deref(), Some("cam.front"));
}

#[test]
fn a_workload_that_is_gone_is_forgotten() {
    let mut view = three();
    let workload = needs("workload.detector", &[]);
    let first = view.choose(&workload).expect("a node");
    view.nodes.get_mut(&first).expect("first").live = false;
    let heir = view.choose(&workload).expect("heir");
    let mut coordinator = Coordinator::new(heir, 10_000);
    coordinator.step(&view, &workload, Some(&first), 0);
    assert!(coordinator.next_deadline().is_some());
    coordinator.forget(&workload.name);
    assert_eq!(coordinator.next_deadline(), None);
}
