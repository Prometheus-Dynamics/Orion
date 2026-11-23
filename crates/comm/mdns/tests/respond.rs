//! The responder without sockets: what it sends, given what it hears and the time.

use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use orion_mdns::packet::{
    A, ANY, Data, FLUSH, IN, Message, Name, PTR, Question, Record, SRV, UNICAST,
};
use orion_mdns::{
    Announce, Interface, MAX_CLUSTER, ORION, OrionTxt, Reach, Responder, SSH, Send, Service,
    orion_txt, tag,
};

fn announce(name: &str) -> Announce {
    Announce {
        name: name.to_owned(),
        tag: "ab12".to_owned(),
        services: vec![
            Service {
                kind: ORION,
                port: 9200,
                txt: vec![b"v=2".to_vec()],
                reach: Reach::everywhere(),
            },
            Service {
                kind: SSH,
                port: 22,
                txt: Vec::new(),
                reach: Reach::everywhere(),
            },
        ],
    }
}

fn interface(index: u32, last: u8) -> Interface {
    Interface {
        name: format!("eth{index}"),
        index,
        v4: vec![Ipv4Addr::new(10, 0, 0, last)],
        v6: Vec::new(),
    }
}

fn name(text: &str) -> Name {
    Name::dotted(text).expect("a name")
}

/// Another host on the network, at 10.0.0.9.
fn other(port: u16) -> SocketAddr {
    SocketAddr::from(([10, 0, 0, 9], port))
}

/// Runs `responder` from `now` to `end`: what it sent, and when.
fn run(responder: &mut Responder, mut now: Instant, end: Instant) -> Vec<(Instant, Send)> {
    let mut sent = Vec::new();
    loop {
        let due = responder.due().max(now);
        if due > end {
            return sent;
        }
        now = due;
        sent.extend(responder.poll(now).into_iter().map(|send| (now, send)));
    }
}

/// A responder named `cam` on interface 2 at 10.0.0.2, its names claimed.
fn claimed(start: Instant) -> (Responder, Instant) {
    let mut responder = Responder::new(announce("cam"), start, 7);
    responder.interfaces(vec![interface(2, 2)], start);
    let end = start + Duration::from_secs(3);
    run(&mut responder, start, end);
    assert!(responder.up(), "it claimed its names");
    (responder, end)
}

fn probes(sent: &[(Instant, Send)]) -> Vec<(Instant, &Send)> {
    sent.iter()
        .filter(|(_, send)| !send.message.response)
        .map(|(at, send)| (*at, send))
        .collect()
}

fn asked(send: &Send) -> Vec<String> {
    send.message
        .questions
        .iter()
        .map(|q| q.name.to_string())
        .collect()
}

fn query(questions: &[(&str, u16, u16)]) -> Message {
    Message {
        questions: questions
            .iter()
            .map(|&(text, kind, class)| Question {
                name: name(text),
                kind,
                class,
            })
            .collect(),
        ..Message::default()
    }
}

fn a(text: &str, last: u8) -> Record {
    Record {
        name: name(text),
        class: IN | FLUSH,
        ttl: 120,
        data: Data::A([10, 0, 0, last]),
    }
}

#[test]
fn it_probes_three_times_a_quarter_second_apart_then_announces_twice_a_second_apart() {
    let start = Instant::now();
    let mut responder = Responder::new(announce("cam"), start, 7);
    responder.interfaces(vec![interface(2, 2)], start);
    let sent = run(&mut responder, start, start + Duration::from_secs(4));
    let probes = probes(&sent);
    assert_eq!(probes.len(), 3, "three probes: {sent:?}");
    assert!(probes[0].0 - start <= Duration::from_millis(250));
    for pair in probes.windows(2) {
        assert_eq!(pair[1].0 - pair[0].0, Duration::from_millis(250));
    }
    // The first asks for its answer to be sent to it alone; all name what it will claim.
    assert!(
        probes[0]
            .1
            .message
            .questions
            .iter()
            .all(|q| q.class & UNICAST != 0)
    );
    assert!(
        probes[1]
            .1
            .message
            .questions
            .iter()
            .all(|q| q.class & UNICAST == 0)
    );
    assert_eq!(
        asked(probes[0].1),
        [
            "cam.local.",
            "cam._orion._tcp.local.",
            "cam._ssh._tcp.local."
        ]
    );
    assert!(probes[0].1.message.authority.contains(&a("cam.local", 2)));
    let announced: Vec<_> = sent.iter().filter(|(_, s)| s.message.response).collect();
    assert_eq!(announced.len(), 2, "two announcements");
    assert_eq!(announced[1].0 - announced[0].0, Duration::from_secs(1));
    let answers = &announced[0].1.message.answers;
    assert!(answers.contains(&a("cam.local", 2)));
    assert!(answers.iter().any(|r| r.name == name("_orion._tcp.local")
        && r.data == Data::Ptr(name("cam._orion._tcp.local"))));
    assert!(
        answers
            .iter()
            .any(|r| r.name == name("_services._dns-sd._udp.local")
                && r.data == Data::Ptr(name("_ssh._tcp.local")))
    );
    assert!(
        answers.iter().any(|r| r.name == name("cam._ssh._tcp.local")
            && matches!(r.data, Data::Srv { port: 22, .. }))
    );
    assert_eq!(responder.name(), "cam");
}

#[test]
fn a_name_another_answers_for_is_given_up_for_the_one_tagged_with_the_key() {
    let start = Instant::now();
    let mut responder = Responder::new(announce("cam"), start, 7);
    responder.interfaces(vec![interface(2, 2)], start);
    let at = responder.due();
    assert_eq!(responder.poll(at).len(), 1, "its first probe");
    let now = at + Duration::from_millis(50);
    let rival = Message {
        response: true,
        answers: vec![a("cam.local", 9)],
        ..Message::default()
    };
    responder.heard(2, other(5353), &rival, now);
    assert_eq!(responder.name(), "cam-ab12");
    assert!(!responder.up());
    let sent = run(&mut responder, now, now + Duration::from_secs(4));
    let again = probes(&sent);
    assert_eq!(again.len(), 3, "it probes for the new name");
    assert_eq!(asked(again[0].1)[0], "cam-ab12.local.");
    assert!(responder.up());
    assert_eq!(responder.name(), "cam-ab12");
}

#[test]
fn a_name_taken_after_it_was_claimed_is_given_up_too() {
    let (mut responder, now) = claimed(Instant::now());
    let rival = Message {
        response: true,
        answers: vec![Record {
            name: name("cam._orion._tcp.local"),
            class: IN | FLUSH,
            ttl: 120,
            data: Data::Srv {
                priority: 0,
                weight: 0,
                port: 1,
                target: name("elsewhere.local"),
            },
        }],
        ..Message::default()
    };
    responder.heard(2, other(5353), &rival, now);
    assert!(!responder.up());
    assert_eq!(responder.name(), "cam-ab12");
}

#[test]
fn what_it_hears_from_itself_or_says_as_it_does_is_no_rival() {
    let (mut responder, now) = claimed(Instant::now());
    // Its own address, come back: whatever it says.
    let echo = Message {
        response: true,
        answers: vec![a("cam.local", 99)],
        ..Message::default()
    };
    responder.heard(2, SocketAddr::from(([10, 0, 0, 2], 5353)), &echo, now);
    // Another saying just what it says (a proxy, a cache); and one saying goodbye.
    let same = Message {
        response: true,
        answers: vec![a("cam.local", 2)],
        ..Message::default()
    };
    responder.heard(2, other(5353), &same, now);
    let mut goodbye = a("cam.local", 9);
    goodbye.ttl = 0;
    let gone = Message {
        response: true,
        answers: vec![goodbye],
        ..Message::default()
    };
    responder.heard(2, other(5353), &gone, now);
    assert!(responder.up());
    assert_eq!(responder.name(), "cam");
}

fn probe_by_other(last: u8) -> Message {
    Message {
        authority: vec![a("cam.local", last)],
        ..query(&[("cam.local", ANY, IN)])
    }
}

#[test]
fn of_two_probing_at_once_the_lesser_waits_a_second_and_the_greater_goes_on() {
    let start = Instant::now();
    let losing = |last: u8| {
        let mut responder = Responder::new(announce("cam"), start, 7);
        responder.interfaces(vec![interface(2, 2)], start);
        let at = responder.due();
        assert_eq!(responder.poll(at).len(), 1, "its first probe");
        responder.heard(2, other(5353), &probe_by_other(last), at);
        let sent = run(&mut responder, at, at + Duration::from_secs(5));
        let next = probes(&sent).first().map(|(when, _)| *when - at);
        (responder, next.expect("it probes again"))
    };
    // 10.0.0.200 is greater than its 10.0.0.2: it waits a second, and keeps its name.
    let (lost, next) = losing(200);
    assert!(next >= Duration::from_secs(1), "waited {next:?}");
    assert_eq!(lost.name(), "cam");
    assert!(lost.up());
    // 10.0.0.1 is lesser: the other waits, and it goes on a quarter second later.
    let (won, next) = losing(1);
    assert_eq!(next, Duration::from_millis(250));
    assert!(won.up());
}

fn answered(sent: &[(Instant, Send)]) -> Vec<&Send> {
    sent.iter()
        .filter(|(_, send)| send.message.response)
        .map(|(_, send)| send)
        .collect()
}

#[test]
fn a_browse_is_answered_to_the_group_a_little_later_with_what_it_needs() {
    let (mut responder, now) = claimed(Instant::now());
    responder.heard(
        2,
        other(5353),
        &query(&[("_orion._tcp.local", PTR, IN)]),
        now,
    );
    assert!(answered(&run(&mut responder, now, now + Duration::from_millis(19))).is_empty());
    let sent = run(&mut responder, now, now + Duration::from_millis(120));
    let replies = answered(&sent);
    assert_eq!(replies.len(), 1);
    let reply = replies[0];
    assert_eq!(reply.to, None, "to the group");
    assert_eq!(reply.message.answers.len(), 1);
    let more = &reply.message.additional;
    assert!(more.iter().any(|r| r.data.kind() == SRV));
    assert!(
        more.iter()
            .any(|r| matches!(&r.data, Data::Txt(t) if t == &[b"v=2".to_vec()]))
    );
    assert!(more.contains(&a("cam.local", 2)));
}

#[test]
fn an_answer_the_asker_already_has_is_not_sent() {
    let (mut responder, now) = claimed(Instant::now());
    let mut knowing = query(&[("_orion._tcp.local", PTR, IN)]);
    knowing.answers.push(Record {
        name: name("_orion._tcp.local"),
        class: IN,
        ttl: 4500,
        data: Data::Ptr(name("cam._orion._tcp.local")),
    });
    responder.heard(2, other(5353), &knowing, now);
    assert!(answered(&run(&mut responder, now, now + Duration::from_secs(1))).is_empty());
}

#[test]
fn a_plain_dns_client_is_answered_alone_with_its_id_and_short_lives() {
    let (mut responder, now) = claimed(Instant::now());
    let mut asked = query(&[("cam.local", A, IN)]);
    asked.id = 77;
    responder.heard(2, other(40000), &asked, now);
    let sent = run(&mut responder, now, now);
    let reply = answered(&sent)[0];
    assert_eq!(reply.to, Some(other(40000)));
    assert_eq!(reply.message.id, 77);
    assert_eq!(reply.message.questions, asked.questions);
    let record = &reply.message.answers[0];
    assert_eq!(record.data, Data::A([10, 0, 0, 2]));
    assert!(record.ttl <= 10);
    assert_eq!(record.class & FLUSH, 0);
}

#[test]
fn a_question_that_asks_to_be_answered_alone_is() {
    let (mut responder, now) = claimed(Instant::now());
    responder.heard(
        2,
        other(5353),
        &query(&[("cam.local", A, IN | UNICAST)]),
        now,
    );
    let sent = run(&mut responder, now, now);
    assert_eq!(answered(&sent)[0].to, Some(other(5353)));
}

#[test]
fn a_record_goes_to_the_group_once_a_second_at_most() {
    let (mut responder, now) = claimed(Instant::now());
    let later = now + Duration::from_secs(2);
    let ask = query(&[("cam.local", A, IN)]);
    responder.heard(2, other(5353), &ask, later);
    assert_eq!(answered(&run(&mut responder, later, later)).len(), 1);
    let soon = later + Duration::from_millis(500);
    responder.heard(2, other(5353), &ask, soon);
    assert!(answered(&run(&mut responder, soon, soon)).is_empty());
    let after = later + Duration::from_millis(1001);
    responder.heard(2, other(5353), &ask, after);
    assert_eq!(answered(&run(&mut responder, after, after)).len(), 1);
}

#[test]
fn a_probe_for_its_name_is_answered_at_once() {
    let (mut responder, now) = claimed(Instant::now());
    let later = now + Duration::from_secs(2);
    responder.heard(2, other(5353), &probe_by_other(9), later);
    let sent = run(&mut responder, later, later);
    let reply = answered(&sent)[0];
    assert_eq!(reply.to, None);
    assert!(reply.message.answers.contains(&a("cam.local", 2)));
    assert!(responder.up(), "a probe takes nothing from it");
}

#[test]
fn each_interface_is_answered_with_its_own_addresses() {
    let start = Instant::now();
    let mut responder = Responder::new(announce("cam"), start, 7);
    responder.interfaces(vec![interface(2, 2), interface(3, 3)], start);
    let now = start + Duration::from_secs(4);
    run(&mut responder, start, now);
    responder.heard(3, other(40000), &query(&[("cam.local", A, IN)]), now);
    let sent = run(&mut responder, now, now);
    let reply = answered(&sent)[0];
    assert_eq!(reply.interface, 3);
    assert_eq!(
        reply.message.answers,
        vec![Record {
            ttl: 10,
            class: IN,
            ..a("cam.local", 3)
        }]
    );
}

#[test]
fn a_new_interface_is_announced_on() {
    let (mut responder, now) = claimed(Instant::now());
    responder.interfaces(vec![interface(2, 2), interface(5, 5)], now);
    let sent = run(&mut responder, now, now + Duration::from_secs(2));
    let on: Vec<u32> = answered(&sent).iter().map(|send| send.interface).collect();
    assert_eq!(on, [5, 5]);
}

#[test]
fn it_says_goodbye_with_every_record_at_no_life() {
    let (responder, _) = claimed(Instant::now());
    let goodbye = responder.goodbye();
    assert_eq!(goodbye.len(), 1);
    assert!(!goodbye[0].message.answers.is_empty());
    assert!(goodbye[0].message.answers.iter().all(|r| r.ttl == 0));
}

#[test]
fn a_long_name_given_up_still_fits_a_label() {
    let start = Instant::now();
    let long = "a".repeat(63);
    let mut responder = Responder::new(announce(&long), start, 7);
    responder.interfaces(vec![interface(2, 2)], start);
    let rival = Message {
        response: true,
        answers: vec![a(&format!("{long}.local"), 9)],
        ..Message::default()
    };
    responder.heard(2, other(5353), &rival, start);
    assert_eq!(responder.name().len(), 63);
    assert!(responder.name().ends_with("-ab12"));
}

#[test]
fn a_message_reads_back_as_it_was_written() {
    let (responder, _) = claimed(Instant::now());
    for send in responder.goodbye() {
        let bytes = send.message.write();
        assert_eq!(Message::read(&bytes), Ok(send.message.clone()));
    }
    let asked = query(&[("cam.local", A, IN | UNICAST)]);
    assert_eq!(Message::read(&asked.write()), Ok(asked));
}

#[test]
fn a_name_is_read_through_its_pointers_and_a_loop_is_refused() {
    // A question for `cam.local`, then a record named by a pointer to it.
    let mut bytes = vec![0, 0, 0x84, 0, 0, 1, 0, 1, 0, 0, 0, 0];
    bytes.extend_from_slice(b"\x03cam\x05local\x00\x00\x01\x00\x01");
    bytes.extend_from_slice(&[0xC0, 12, 0, 1, 0, 1, 0, 0, 0, 120, 0, 4, 10, 0, 0, 7]);
    let message = Message::read(&bytes).expect("read");
    assert_eq!(message.answers[0].name, name("CAM.local"));
    assert_eq!(message.answers[0].data, Data::A([10, 0, 0, 7]));
    let mut looped = vec![0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0];
    looped.extend_from_slice(&[0xC0, 12, 0, 1, 0, 1]);
    assert!(Message::read(&looped).is_err());
    let lying = [0, 0, 0, 0, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0];
    assert!(Message::read(&lying).is_err());
    for end in 0..bytes.len() {
        assert!(Message::read(&bytes[..end]).is_err(), "cut at {end}");
    }
}

fn txt<'a>(cluster: &'a str, node: &'a str, key: &'a [u8; 32]) -> OrionTxt<'a> {
    OrionTxt {
        node,
        key,
        wire: 255,
        revision: u16::MAX,
        level: u16::MAX,
        cluster,
        port: u16::MAX,
        state: "flasher",
        security: "damaged",
    }
}

#[test]
fn the_orion_txt_record_fits_and_an_older_reader_reads_it() {
    let key = [0xAB; 32];
    let node = "n".repeat(63);
    let cluster = "c".repeat(MAX_CLUSTER);
    let strings = orion_txt(&txt(&cluster, &node, &key)).expect("fits");
    assert!(strings.iter().all(|s| s.len() <= 255));
    let size: usize = strings.iter().map(|s| s.len() + 1).sum();
    assert!(size <= 465, "{size} bytes");
    let too_long = "c".repeat(MAX_CLUSTER + 1);
    let why = orion_txt(&txt(&too_long, &node, &key)).expect_err("too long");
    assert!(why.contains("253 bytes"), "{why}");
    // What a node that knows only layout 2 reads of it: all it knows, the rest left by name.
    let pairs: Vec<(String, String)> = orion_txt(&txt("lab", "cam", &key))
        .expect("fits")
        .iter()
        .map(|s| {
            let text = String::from_utf8(s.clone()).expect("text");
            let (key, value) = text.split_once('=').expect("key=value");
            (key.to_owned(), value.to_owned())
        })
        .collect();
    let read = orion_discovery::Advertisement::from_txt(&pairs).expect("an older reader reads it");
    assert_eq!(read.node.to_string(), "cam");
    assert_eq!(read.cluster, "lab");
    assert_eq!(read.public_key, key);
    assert_eq!(read.port, u16::MAX);
    assert_eq!(
        tag(&key),
        orion_identity::fingerprint(&key)[7..11].to_owned()
    );
}

#[test]
fn a_service_is_announced_only_where_it_can_be_reached() {
    let start = Instant::now();
    let mut kept = announce("cam");
    // The network way in on eth3's address alone; SSH kept to eth2 by name.
    kept.services[0].reach = Reach {
        addr: "10.0.0.3".parse().expect("addr"),
        interfaces: Vec::new(),
    };
    kept.services[1].reach = Reach {
        addr: "0.0.0.0".parse().expect("addr"),
        interfaces: vec!["eth2".to_owned()],
    };
    // eth4 reaches neither: nothing is said there, not even the name.
    let mut responder = Responder::new(kept, start, 7);
    responder.interfaces(
        vec![interface(2, 2), interface(3, 3), interface(4, 4)],
        start,
    );
    let sent = run(&mut responder, start, start + Duration::from_secs(4));
    let kinds = |on: u32| -> Vec<String> {
        answered(&sent)
            .iter()
            .filter(|send| send.interface == on)
            .flat_map(|send| &send.message.answers)
            .filter(|r| r.data.kind() == PTR && r.name.0[0] != b"_services")
            .map(|r| r.name.to_string())
            .collect()
    };
    assert_eq!(kinds(2), ["_ssh._tcp.local.", "_ssh._tcp.local."]);
    assert_eq!(kinds(3), ["_orion._tcp.local.", "_orion._tcp.local."]);
    assert!(sent.iter().all(|(_, send)| send.interface != 4), "{sent:?}");
    // Loopback alone reaches nothing.
    let mut alone = announce("cam");
    for service in &mut alone.services {
        service.reach.addr = "127.0.0.1".parse().expect("addr");
    }
    assert!(!alone.reaches(&interface(2, 2)));
}
