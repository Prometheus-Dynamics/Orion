//! A doorbell with a real eventfd, a real ring and real threads.

#![allow(unsafe_code)]

use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use orion_ring::{FieldId, Flags, FrameTime, Next, Ring, Spec};
use orion_ring_doorbell::{Bell, Doorbell, Wake};

fn readable(doorbell: &Doorbell) -> bool {
    let mut poll = libc::pollfd {
        fd: std::os::fd::AsFd::as_fd(doorbell).as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: one valid `pollfd`, zero timeout.
    unsafe { libc::poll(&raw mut poll, 1, 0) > 0 }
}

#[test]
fn only_a_sleeping_subscriber_is_rung_and_only_once_per_sleep() {
    let mut doorbell = Doorbell::create().expect("doorbell");
    let mut bell = doorbell.bell().expect("bell");
    // Awake: nothing happens.
    assert!(!bell.ring());
    assert!(!readable(&doorbell));
    // Asleep: one ring, however many samples follow.
    doorbell.arm();
    assert!(bell.ring());
    assert!(!bell.ring());
    assert!(readable(&doorbell));
    // Awake again: the ring is taken out and nothing more comes.
    doorbell.disarm();
    assert!(!readable(&doorbell));
    assert!(!bell.ring());
    // The next sleep is a new one.
    doorbell.arm();
    assert!(bell.ring());
    doorbell.disarm();
}

#[test]
fn wait_does_not_sleep_when_there_is_something_to_read() {
    let mut doorbell = Doorbell::create().expect("doorbell");
    assert_eq!(doorbell.wait(None, || true).expect("wait"), Wake::Ready);
    // Ready only on the second look, as when a sample lands just before the sleep.
    let mut looks = 0;
    let wake = doorbell.wait(None, || {
        looks += 1;
        looks == 2
    });
    assert_eq!((wake.expect("wait"), looks), (Wake::Ready, 2));
    let started = std::time::Instant::now();
    let wake = doorbell.wait(Some(Duration::from_millis(30)), || false);
    assert_eq!(wake.expect("wait"), Wake::TimedOut);
    assert!(started.elapsed() >= Duration::from_millis(30));
    // And it is awake afterwards.
    assert!(!doorbell.bell().expect("bell").ring());
}

#[test]
fn several_publishers_ring_one_doorbell_and_cannot_write_its_page() {
    let mut doorbell = Doorbell::create().expect("doorbell");
    let mut first = doorbell.bell().expect("bell");
    let mut second = doorbell.bell().expect("bell");
    doorbell.arm();
    assert!(first.ring());
    assert!(second.ring());
    doorbell.disarm();
    let (_, page) = doorbell.bell_fds().expect("fds");
    assert!(orion_ring::WriteMap::new(page).is_err());
}

#[test]
fn a_doorbell_nobody_answers_never_holds_the_publisher_up() {
    let mut doorbell = Doorbell::create().expect("doorbell");
    let (eventfd, page) = doorbell.bell_fds().expect("fds");
    // Fill the counter, so a write to it would block if it could.
    let nearly_full = (u64::MAX - 1).to_ne_bytes();
    // SAFETY: an open descriptor and eight readable bytes.
    assert_eq!(
        unsafe { libc::write(eventfd.as_raw_fd(), nearly_full.as_ptr().cast(), 8) },
        8
    );
    let mut bell = Bell::attach(eventfd, page).expect("bell");
    doorbell.arm();
    assert!(bell.ring());
    // And a subscriber that is gone.
    let mut gone = Doorbell::create().expect("doorbell");
    let mut orphan = gone.bell().expect("bell");
    gone.arm();
    drop(gone);
    assert!(orphan.ring());
}

/// Reads everything there is, counting what was read or skipped.
fn take(reader: &mut orion_ring::Reader, seen: &mut u64, next: &mut Option<u64>) {
    loop {
        match reader.next() {
            Next::Sample(sample) => {
                assert!(next.is_none_or(|next| sample.seq >= next));
                *next = Some(sample.seq + 1);
                *seen += 1;
            }
            Next::Skipped(count) => *seen += count,
            Next::Empty => break,
        }
    }
}

/// A subscriber that sleeps whenever it has caught up, against a publisher that publishes in
/// bursts with pauses. If a wake-up were ever lost the subscriber would sleep through a sample
/// until its timeout, which fails the test.
#[test]
fn no_wake_up_is_lost() {
    const SAMPLES: u64 = 200_000;
    let ring = Ring::create(&Spec {
        fields: 1,
        history: 1024,
        max_payload: 8,
        expiry_ms: 0,
    })
    .expect("ring");
    let mut doorbell = Doorbell::create().expect("doorbell");
    let mut bell = doorbell.bell().expect("bell");
    let mut writer = ring.writer().expect("writer");
    let mut reader = ring.reader().expect("reader");
    let rings = AtomicU64::new(0);
    let sleeps = AtomicU64::new(0);

    std::thread::scope(|scope| {
        let subscriber = scope.spawn(|| {
            let (mut seen, mut next) = (0_u64, None);
            while seen < SAMPLES {
                take(&mut reader, &mut seen, &mut next);
                if seen >= SAMPLES {
                    break;
                }
                let before = seen;
                let wake = doorbell
                    .wait(Some(Duration::from_secs(20)), || {
                        take(&mut reader, &mut seen, &mut next);
                        seen > before
                    })
                    .expect("wait");
                assert_ne!(wake, Wake::TimedOut, "slept through a sample at {seen}");
                if wake != Wake::Ready {
                    sleeps.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        let mut random = 0x9E37_79B9_7F4A_7C15_u64;
        for seq in 0..SAMPLES {
            writer
                .publish(
                    FieldId(0),
                    FrameTime::from_nanos(seq),
                    Flags::NONE,
                    &seq.to_le_bytes(),
                )
                .expect("publish");
            if bell.ring() {
                rings.fetch_add(1, Ordering::Relaxed);
            }
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            // Mostly back to back, sometimes a pause long enough for the subscriber to sleep.
            match random % 512 {
                0 => std::thread::sleep(Duration::from_micros(200)),
                1..=40 => std::thread::yield_now(),
                _ => {}
            }
        }
        subscriber.join().expect("subscriber");
    });
    let (rings, sleeps) = (rings.into_inner(), sleeps.into_inner());
    assert!(
        rings > 0 && sleeps > 0,
        "the subscriber never slept: {rings} rings, {sleeps} sleeps"
    );
    assert!(rings < SAMPLES / 2, "rang on almost every publish: {rings}");
}
