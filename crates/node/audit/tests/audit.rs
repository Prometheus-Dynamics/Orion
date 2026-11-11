//! The audit log under load: it rotates between whole lines, stays inside its cap, never
//! holds the caller up, and says what it could not keep.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use orion_audit::{Audit, AuditConfig, Entry, Who};

fn dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-audit-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn files(path: &Path, keep: u32) -> Vec<PathBuf> {
    let mut all = vec![path.to_owned()];
    for number in 1..=keep + 2 {
        let numbered = PathBuf::from(format!("{}.{number}", path.display()));
        if numbered.exists() {
            all.push(numbered);
        }
    }
    all
}

/// The value of `"key":"..."` in a line.
fn field<'a>(line: &'a str, key: &str) -> &'a str {
    let start = line.find(&format!("\"{key}\":\"")).expect(key) + key.len() + 4;
    let end = line[start..].find('"').expect("closed") + start;
    &line[start..end]
}

#[test]
fn it_rotates_under_load_and_every_line_is_whole() {
    let dir = dir("load");
    let path = dir.join("log/audit.log");
    let config = AuditConfig {
        path: path.clone(),
        max_bytes: 16 * 1024,
        keep: 3,
        queue: 256,
    };
    let audit = Arc::new(Audit::start("board", config).expect("start"));
    const THREADS: u32 = 4;
    const EACH: u32 = 20_000;
    let started = Instant::now();
    let writers: Vec<_> = (0..THREADS)
        .map(|thread| {
            let audit = audit.clone();
            std::thread::spawn(move || {
                let mut slowest = Duration::ZERO;
                for number in 0..EACH {
                    let what = format!("t{thread}/{number}");
                    let before = Instant::now();
                    audit.record(&Entry {
                        time_ms: 1_700_000_000_000 + u64::from(number),
                        who: Who::Local(1000 + thread),
                        did: "put",
                        what: &what,
                        result: "ok",
                    });
                    slowest = slowest.max(before.elapsed());
                }
                slowest
            })
        })
        .collect();
    let slowest = writers
        .into_iter()
        .map(|writer| writer.join().expect("writer"))
        .max()
        .expect("threads");
    audit.flush();
    let total = u64::from(THREADS * EACH);
    // Nothing is lost without being counted.
    assert_eq!(audit.written() + audit.dropped(), total);
    assert!(audit.written() > 0);
    // Recording never waited for the disk. A scheduler can hold a thread a while; a disk
    // write per entry would hold every one of them.
    assert!(slowest < Duration::from_millis(250), "{slowest:?}");
    println!(
        "{} written, {} dropped in {:?}",
        audit.written(),
        audit.dropped(),
        started.elapsed()
    );

    // The files: no more than the one and those kept, none past the cap.
    let kept = files(&path, 3);
    assert_eq!(kept.len(), 4, "{kept:?}");
    let mut newest_seen: Vec<Option<u32>> = vec![None; THREADS as usize];
    // Oldest first.
    for file in kept.iter().rev() {
        let text = std::fs::read_to_string(file).expect("a file of text");
        assert!(text.len() as u64 <= 16 * 1024, "{} bytes", text.len());
        assert!(text.ends_with('\n'), "a file ends between lines");
        for line in text.lines() {
            assert!(
                line.starts_with("{\"time_ms\":") && line.ends_with('}'),
                "{line}"
            );
            assert_eq!(field(line, "node"), "board");
            if field(line, "did") == "dropped" {
                assert_eq!(field(line, "who"), "orion");
                continue;
            }
            // Each thread's entries are in the order it made them, across files.
            let (thread, number) = field(line, "what").split_once('/').expect("t/n");
            let thread: usize = thread[1..].parse().expect("thread");
            let number: u32 = number.parse().expect("number");
            assert!(newest_seen[thread].is_none_or(|before| before < number));
            newest_seen[thread] = Some(number);
        }
    }
    if audit.dropped() > 0 {
        // What was dropped is said in the log, if that line is still among those kept.
        let said: u64 = kept
            .iter()
            .flat_map(|file| {
                std::fs::read_to_string(file)
                    .expect("text")
                    .lines()
                    .filter(|line| field(line, "did") == "dropped")
                    .map(|line| {
                        field(line, "what")
                            .split(' ')
                            .next()
                            .expect("count")
                            .parse::<u64>()
                            .expect("a number")
                    })
                    .collect::<Vec<_>>()
            })
            .sum();
        assert!(said <= audit.dropped());
    }
}

#[test]
fn a_line_is_one_json_object_whatever_is_in_it() {
    let line = Entry {
        time_ms: 5,
        who: Who::Operator("ab\"cd"),
        did: "run",
        what: "a \"quoted\" name\nwith a line\tand \\ and \u{1}",
        result: "denied",
    }
    .line("left");
    assert_eq!(
        line,
        "{\"time_ms\":5,\"node\":\"left\",\"who\":\"operator:ab\\\"cd\",\"did\":\"run\",\
         \"what\":\"a \\\"quoted\\\" name\\nwith a line\\tand \\\\ and \\u0001\",\
         \"result\":\"denied\"}\n"
    );
    for (who, text) in [
        (Who::Local(0), "local:0"),
        (Who::Peer("right"), "peer:right"),
        (Who::Device("imu"), "device:imu"),
        (Who::Anonymous, "anonymous"),
        (Who::Orion, "orion"),
    ] {
        let line = Entry {
            time_ms: 0,
            who,
            did: "x",
            what: "",
            result: "ok",
        }
        .line("n");
        assert_eq!(field(&line, "who"), text);
    }
}

#[test]
fn it_goes_on_in_the_file_that_was_there() {
    let dir = dir("again");
    let path = dir.join("audit.log");
    let entry = |what: &'static str| Entry {
        time_ms: 1,
        who: Who::Local(0),
        did: "put",
        what,
        result: "ok",
    };
    let config = AuditConfig {
        max_bytes: 300,
        keep: 1,
        ..AuditConfig::new(&path)
    };
    let audit = Audit::start("board", config.clone()).expect("start");
    audit.record(&entry("first"));
    audit.record(&entry("second"));
    // Dropping it writes what was waiting.
    drop(audit);
    let audit = Audit::start("board", config).expect("start");
    audit.record(&entry("third"));
    audit.flush();
    let text = std::fs::read_to_string(&path).expect("log");
    let whats: Vec<&str> = text.lines().map(|line| field(line, "what")).collect();
    assert_eq!(whats, ["first", "second", "third"]);
    // The next line does not fit: the file is rotated, and with one kept, the older goes.
    audit.record(&entry("fourth"));
    audit.record(&entry("fifth"));
    audit.record(&entry("sixth"));
    audit.record(&entry("seventh"));
    audit.flush();
    assert_eq!(files(&path, 1).len(), 2);
    let now = std::fs::read_to_string(&path).expect("log");
    assert!(now.lines().all(|line| field(line, "what") != "first"));
    assert_eq!(
        field(now.lines().last().expect("a line"), "what"),
        "seventh"
    );
}

#[test]
fn with_nothing_kept_the_file_is_emptied() {
    let dir = dir("none");
    let path = dir.join("audit.log");
    let audit = Audit::start(
        "board",
        AuditConfig {
            max_bytes: 200,
            keep: 0,
            ..AuditConfig::new(&path)
        },
    )
    .expect("start");
    for number in 0..50 {
        audit.record(&Entry {
            time_ms: number,
            who: Who::Local(0),
            did: "put",
            what: "x",
            result: "ok",
        });
        audit.flush();
    }
    assert_eq!(files(&path, 0).len(), 1);
    assert!(std::fs::metadata(&path).expect("log").len() <= 200);
}
