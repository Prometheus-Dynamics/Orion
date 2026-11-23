//! The browse without sockets: what it asks and when, and what it keeps of what it hears,
//! heard from a real responder where one will do.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use orion_mdns::packet::{Data, FLUSH, IN, Message, Name, PTR, Question, Record, UNICAST};
use orion_mdns::{
    AGAIN, ASK, Announce, Browser, Interface, KEEP, MOST, MOST_ADDRESSES, MOST_TXT, ORION, Reach,
    Responder, Send, Service,
};

fn name(text: &str) -> Name {
    Name::dotted(text).expect("a name")
}

/// A responder named `name` on interface 2 at 10.0.0.`last`, its names claimed, saying `txt`.
fn board(label: &str, last: u8, txt: &[&str], start: Instant) -> Responder {
    let mut responder = Responder::new(
        Announce {
            name: label.to_owned(),
            tag: "ab12".to_owned(),
            services: vec![Service {
                kind: ORION,
                port: 9200,
                txt: txt.iter().map(|s| s.as_bytes().to_vec()).collect(),
                reach: Reach::everywhere(),
            }],
        },
        start,
        u64::from(last),
    );
    responder.interfaces(
        vec![Interface {
            name: "eth2".to_owned(),
            index: 2,
            v4: vec![Ipv4Addr::new(10, 0, 0, last)],
            v6: Vec::new(),
        }],
        start,
    );
    say(&mut responder, start, start + Duration::from_secs(4));
    assert!(responder.up(), "it claimed its names");
    responder
}

/// What `responder` sends from `now` to `end`.
fn say(responder: &mut Responder, mut now: Instant, end: Instant) -> Vec<Send> {
    let mut sent = Vec::new();
    loop {
        let due = responder.due().max(now);
        if due > end {
            return sent;
        }
        now = due;
        sent.extend(responder.poll(now));
    }
}

/// What `browser` asks from `now` to `end`, and when.
fn asks(browser: &mut Browser, mut now: Instant, end: Instant) -> Vec<(Instant, Send)> {
    let mut sent = Vec::new();
    // Each turn moves time on or sends: a browser due now that does neither would never be
    // done, and is not one.
    let mut still = 0;
    loop {
        let due = browser.due(now).max(now);
        if due > end {
            return sent;
        }
        let said = browser.poll(due);
        still = if due == now && said.is_empty() {
            still + 1
        } else {
            0
        };
        assert!(
            still < 1000,
            "due at {due:?} again and again, with nothing to do"
        );
        now = due;
        sent.extend(said.into_iter().map(|send| (now, send)));
    }
}

/// The browser at 10.0.0.9, as a responder hears it.
fn asker() -> SocketAddr {
    SocketAddr::from(([10, 0, 0, 9], 5353))
}

/// A browser on interface 2 that has asked its two questions of the start, and when the
/// second was.
fn browsing(start: Instant) -> (Browser, Message, Instant) {
    let mut browser = Browser::new(ORION, 7);
    browser.interfaces(&[2], start);
    let mut asked = asks(&mut browser, start, start + Duration::from_secs(2));
    assert_eq!(
        asked.len(),
        2,
        "a question as the interface comes, and one more"
    );
    let (first, _) = asked.remove(0);
    let (at, send) = asked.remove(0);
    assert!(
        first >= start + Duration::from_millis(20),
        "{:?}",
        first - start
    );
    assert!(
        first <= start + Duration::from_millis(120),
        "{:?}",
        first - start
    );
    assert_eq!(at, first + AGAIN);
    assert_eq!((send.interface, send.to), (2, None), "to the group");
    (browser, send.message, at)
}

/// A response saying all an instance has: `instance` at `host`, with `txt`, living `ttl`.
fn told(instance: &str, host: &str, last: u8, txt: &[&[u8]], ttl: u32) -> Message {
    let service = name("_orion._tcp.local");
    let instance = service.under(instance.as_bytes()).expect("a name");
    let host = name(host);
    Message {
        response: true,
        answers: vec![
            Record {
                name: service,
                class: IN,
                ttl,
                data: Data::Ptr(instance.clone()),
            },
            Record {
                name: host.clone(),
                class: IN | FLUSH,
                ttl,
                data: Data::A([10, 0, 0, last]),
            },
            Record {
                name: instance.clone(),
                class: IN | FLUSH,
                ttl,
                data: Data::Srv {
                    priority: 0,
                    weight: 0,
                    port: 9200,
                    target: host,
                },
            },
            Record {
                name: instance,
                class: IN | FLUSH,
                ttl,
                data: Data::Txt(txt.iter().map(|s| s.to_vec()).collect()),
            },
        ],
        ..Message::default()
    }
}

#[test]
fn a_board_that_answers_the_question_is_found_with_where_it_is_and_what_it_says() {
    let start = Instant::now();
    let mut cam = board("cam", 2, &["v=2", "id=cam", "clock=unset"], start);
    let (mut browser, question, at) = browsing(start + Duration::from_secs(5));
    assert_eq!(
        question,
        Message {
            questions: vec![Question {
                name: name("_orion._tcp.local"),
                kind: PTR,
                class: IN,
            }],
            ..Message::default()
        },
        "one question, asked of everyone, answered to the group"
    );
    assert_eq!(browser.found(), Vec::new());

    cam.heard(2, asker(), &question, at);
    let answers = say(&mut cam, at, at + Duration::from_secs(1));
    assert_eq!(answers.len(), 1, "{answers:?}");
    assert_eq!(answers[0].to, None, "to the group: every browser hears it");
    let heard = at + Duration::from_millis(200);
    browser.heard(2, &answers[0].message, heard);

    let found = browser.found();
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].instance, "cam");
    assert_eq!(found[0].port, 9200);
    assert_eq!(found[0].addresses, [IpAddr::from([10, 0, 0, 2])]);
    assert_eq!(
        found[0].txt,
        [b"v=2".to_vec(), b"id=cam".to_vec(), b"clock=unset".to_vec()]
    );
    assert_eq!(found[0].heard, heard);
    assert_eq!(browser.changes(), 3, "its place, its strings, its address");

    // Heard again, the same: nothing changed, and it is kept from then.
    let again = heard + Duration::from_secs(60);
    browser.heard(2, &answers[0].message, again);
    assert_eq!(browser.changes(), 3);
    assert_eq!(browser.found()[0].heard, again);

    // What it says changes (its clock is set): announced again, and that is what is found.
    cam.retell(
        ORION,
        vec![
            b"v=2".to_vec(),
            b"id=cam".to_vec(),
            b"clock=synced".to_vec(),
        ],
        again,
    );
    for send in say(&mut cam, again, again + Duration::from_secs(3)) {
        browser.heard(2, &send.message, again + Duration::from_secs(1));
    }
    assert_eq!(browser.found()[0].txt[2], b"clock=synced".to_vec());
    assert_eq!(browser.changes(), 4);

    // Its goodbye: gone at once.
    for send in cam.goodbye() {
        browser.heard(2, &send.message, again + Duration::from_secs(5));
    }
    assert_eq!(browser.found(), Vec::new());
    assert_eq!(browser.changes(), 5);
}

#[test]
fn the_question_is_asked_once_a_minute_and_put_off_when_another_asks_it() {
    let start = Instant::now();
    let (mut browser, question, first) = browsing(start);

    // Nobody else asks: one question each ASK, and up to two seconds more.
    let asked = asks(&mut browser, first, first + Duration::from_secs(600));
    assert!((9..=10).contains(&asked.len()), "{} questions", asked.len());
    let mut last = first;
    for (at, send) in &asked {
        let apart = *at - last;
        assert!(
            apart >= ASK && apart <= ASK + Duration::from_secs(2),
            "{apart:?}"
        );
        assert_eq!(send.message, question);
        last = *at;
    }

    // Another asks the same of everyone each half minute: this one never does.
    let mut now = last;
    let end = now + Duration::from_secs(600);
    while now < end {
        now += Duration::from_secs(30);
        assert_eq!(
            asks(&mut browser, now, now),
            Vec::new(),
            "asked all the same"
        );
        browser.heard(2, &question, now);
    }

    // Not put off by a question that is not that one: on another interface, for an answer
    // to the asker alone, or with the answers it knows, which the boards then leave out.
    let quiet = |mutate: &dyn Fn(&mut Message)| {
        let mut other = question.clone();
        mutate(&mut other);
        other
    };
    let unicast = quiet(&|m| m.questions[0].class |= UNICAST);
    let knowing = quiet(&|m| m.answers = told("cam", "cam.local", 2, &[b"v=2"], 4500).answers);
    let elsewhere = quiet(&|m| m.questions[0].name = name("_ssh._tcp.local"));
    let due = browser.due(now);
    for at in [10, 20, 30, 40, 50] {
        let at = now + Duration::from_secs(at);
        browser.heard(2, &unicast, at);
        browser.heard(2, &knowing, at);
        browser.heard(2, &elsewhere, at);
        browser.heard(3, &question, at);
    }
    assert_eq!(browser.due(now), due);
    assert_eq!(asks(&mut browser, now, due).len(), 1);

    // An interface that goes is not asked on; one that comes is, shortly, and once more
    // though another asks in between: this one has heard nothing there yet.
    browser.interfaces(&[4], due);
    let first = asks(&mut browser, due, due + Duration::from_millis(120));
    assert_eq!(first.len(), 1, "{first:?}");
    let at = first[0].0;
    browser.heard(4, &question, at + Duration::from_millis(500));
    let asked = asks(&mut browser, at, at + Duration::from_secs(100));
    assert!(
        asked.iter().all(|(_, send)| send.interface == 4),
        "{asked:?}"
    );
    assert_eq!(asked[0].0, at + AGAIN);
    assert_eq!(asked.len(), 2, "{asked:?}");
}

#[test]
fn a_board_not_heard_is_dropped_after_the_keep_time_and_not_before() {
    let start = Instant::now();
    let (mut browser, _, at) = browsing(start);
    browser.heard(2, &told("cam", "cam.local", 2, &[b"v=2"], 4500), at);
    browser.heard(2, &told("arm", "arm.local", 3, &[b"v=2"], 4500), at);
    // One is heard again a minute on; the other never.
    let again = at + Duration::from_secs(60);
    browser.heard(2, &told("arm", "arm.local", 3, &[b"v=2"], 4500), again);

    let names = |browser: &Browser| -> Vec<String> {
        browser.found().into_iter().map(|f| f.instance).collect()
    };
    asks(&mut browser, at, at + KEEP - Duration::from_millis(1));
    assert_eq!(
        names(&browser),
        ["arm", "cam"],
        "kept to the last moment, by name"
    );
    let changes = browser.changes();
    asks(&mut browser, at + KEEP, at + KEEP);
    assert_eq!(names(&browser), ["arm"], "dropped when the time is up");
    assert_eq!(browser.changes(), changes + 1);
    asks(&mut browser, at + KEEP, again + KEEP);
    assert_eq!(names(&browser), Vec::<String>::new());
}

#[test]
fn what_is_kept_is_bounded() {
    let start = Instant::now();
    let (mut browser, _, at) = browsing(start);

    // A TXT record too long is not kept, and its instance is not found.
    let long = vec![b'x'; 255];
    let over: Vec<&[u8]> = vec![&long; MOST_TXT / 255 + 1];
    browser.heard(2, &told("big", "big.local", 2, &over, 4500), at);
    assert_eq!(browser.found(), Vec::new());
    browser.heard(2, &told("big", "big.local", 2, &over[1..], 4500), at);
    assert_eq!(browser.found().len(), 1, "at the bound it is");

    // No more instances than MOST: one more is not kept, and counted.
    for each in 1..MOST {
        let label = format!("cam{each}");
        browser.heard(2, &told(&label, "cams.local", 2, &[b"v=2"], 4500), at);
    }
    assert_eq!(browser.found().len(), MOST);
    assert_eq!(browser.refused(), 0);
    browser.heard(2, &told("late", "late.local", 9, &[b"v=2"], 4500), at);
    assert_eq!(browser.found().len(), MOST);
    assert!(browser.found().iter().all(|found| found.instance != "late"));
    assert_eq!(browser.refused(), 1, "counted once for an announcement");
    // One that is kept is still heard.
    let later = at + Duration::from_secs(1);
    browser.heard(2, &told("cam1", "cams.local", 2, &[b"v=3"], 4500), later);
    let cam1 = browser
        .found()
        .into_iter()
        .find(|found| found.instance == "cam1")
        .expect("kept");
    assert_eq!((cam1.txt, cam1.heard), (vec![b"v=3".to_vec()], later));

    // No more addresses than MOST_ADDRESSES for one.
    for last in 3..40 {
        browser.heard(2, &told("cam1", "cams.local", last, &[b"v=3"], 4500), later);
    }
    let cam1 = browser
        .found()
        .into_iter()
        .find(|found| found.instance == "cam1")
        .expect("kept");
    assert_eq!(cam1.addresses.len(), MOST_ADDRESSES);
}

#[test]
fn an_address_is_of_the_host_its_instance_names_and_goes_when_it_says_so() {
    let start = Instant::now();
    let (mut browser, _, at) = browsing(start);
    browser.heard(2, &told("cam", "cam.local", 2, &[b"v=2"], 4500), at);
    // Another host's address is not this one's.
    let mut stray = told("arm", "arm.local", 7, &[b"v=2"], 4500);
    stray.answers.retain(|r| matches!(r.data, Data::A(_)));
    browser.heard(2, &stray, at);
    assert_eq!(browser.found()[0].addresses, [IpAddr::from([10, 0, 0, 2])]);

    // A second address, and the first said to be gone.
    let mut moved = told("cam", "cam.local", 5, &[b"v=2"], 4500);
    let mut gone = moved.answers[1].clone();
    (gone.ttl, gone.data) = (0, Data::A([10, 0, 0, 2]));
    moved.answers.push(gone);
    browser.heard(2, &moved, at);
    assert_eq!(browser.found()[0].addresses, [IpAddr::from([10, 0, 0, 5])]);

    // The instance on another host: the old host's addresses are not kept for it.
    browser.heard(2, &told("cam", "other.local", 8, &[b"v=2"], 4500), at);
    assert_eq!(browser.found()[0].addresses, [IpAddr::from([10, 0, 0, 8])]);
}

#[test]
fn a_known_board_has_a_place_when_all_are_taken_and_takes_it_from_one_that_is_not() {
    let start = Instant::now();
    let (mut browser, _, at) = browsing(start);
    browser.known(Box::new(|txt| txt.iter().any(|s| s == b"pk=ours")));
    let names = |browser: &Browser| -> Vec<String> {
        browser.found().into_iter().map(|f| f.instance).collect()
    };
    // Every place taken by a name nobody knows, each heard a second after the one before.
    for each in 0..MOST {
        let label = format!("fake{each}");
        let heard = at + Duration::from_secs(each as u64);
        browser.heard(2, &told(&label, "fakes.local", 2, &[b"pk=x"], 4500), heard);
    }
    let late = at + Duration::from_secs(100);
    browser.heard(2, &told("late", "late.local", 9, &[b"pk=y"], 4500), late);
    assert!(
        !names(&browser).contains(&"late".to_owned()),
        "no place for one more"
    );

    // A known board: it is whole at once, in the place of the one longest unheard.
    let changes = browser.changes();
    browser.heard(2, &told("cam", "cam.local", 3, &[b"pk=ours"], 4500), late);
    let found = browser.found();
    assert_eq!(found.len(), MOST);
    let cam = found.iter().find(|f| f.instance == "cam").expect("kept");
    assert_eq!(cam.addresses, [IpAddr::from([10, 0, 0, 3])]);
    assert_eq!(cam.port, 9200);
    assert!(
        !names(&browser).contains(&"fake0".to_owned()),
        "the oldest went"
    );
    assert!(names(&browser).contains(&"fake1".to_owned()));
    assert!(browser.changes() > changes);

    // Known boards do not take each other's places: with every place a known board's, one
    // more is refused.
    for each in 0..MOST {
        let label = format!("cam{each}");
        browser.heard(2, &told(&label, "cams.local", 4, &[b"pk=ours"], 4500), late);
    }
    assert!(names(&browser).iter().all(|name| name.starts_with("cam")));
    let refused = browser.refused();
    browser.heard(2, &told("more", "more.local", 5, &[b"pk=ours"], 4500), late);
    assert!(!names(&browser).contains(&"more".to_owned()));
    assert_eq!(browser.refused(), refused + 1);
}
