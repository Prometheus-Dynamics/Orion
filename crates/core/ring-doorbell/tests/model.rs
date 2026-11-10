//! The doorbell's shared-memory protocol under `loom`: in every interleaving, a subscriber that
//! goes to sleep with a sample unread is rung.
//!
//! The eventfd is a `Notify`: a ring leaves a permit, a sleep takes one or blocks. A lost
//! wake-up is then a subscriber blocked for ever, which the model checker reports as a deadlock.

use std::sync::atomic::Ordering;

use loom::sync::atomic::AtomicU64;
use loom::sync::{Arc, Notify};
use orion_ring::{Mem, MemMut};
use orion_ring_doorbell::protocol::{arm, disarm, wants_ring};

/// Word 0 is the doorbell page. Word 1 stands for the ring's head.
#[derive(Clone)]
struct Model(Arc<[AtomicU64; 2]>);

const HEAD: usize = 1;

impl Mem for Model {
    fn words(&self) -> usize {
        2
    }

    fn load(&self, index: usize, order: Ordering) -> u64 {
        self.0[index].load(order)
    }

    fn fence(order: Ordering) {
        loom::sync::atomic::fence(order);
    }
}

impl MemMut for Model {
    fn store(&self, index: usize, value: u64, order: Ordering) {
        self.0[index].store(value, order);
    }
}

/// Runs a publisher of `samples` samples against a subscriber that sleeps whenever it has caught
/// up, through as many sleeps as it takes.
fn run(samples: u64) {
    let mem = Model(Arc::new([AtomicU64::new(0), AtomicU64::new(0)]));
    let eventfd = Arc::new(Notify::new());
    let publisher = {
        let (mem, eventfd) = (mem.clone(), eventfd.clone());
        loom::thread::spawn(move || {
            let mut rung = 0;
            for sample in 1..=samples {
                // As the ring's writer stores its head.
                mem.store(HEAD, sample, Ordering::Release);
                if wants_ring(&mem, &mut rung) {
                    eventfd.notify();
                }
            }
        })
    };
    let (mut seen, mut sleeps) = (0, 0);
    while seen < samples {
        // As the ring's reader loads the head.
        let head = mem.load(HEAD, Ordering::Acquire);
        if head > seen {
            seen = head;
            continue;
        }
        sleeps += 1;
        arm(&mem, sleeps);
        if mem.load(HEAD, Ordering::Acquire) > seen {
            disarm(&mem, sleeps);
            continue;
        }
        eventfd.wait();
        disarm(&mem, sleeps);
    }
    publisher.join().expect("publisher");
}

#[test]
fn a_subscriber_asleep_with_a_sample_unread_is_always_rung() {
    loom::model(|| run(2));
}

/// One sample, one chance to sleep, and no blocking: the subscriber only decides whether it
/// would sleep, and the publisher whether it rings. A wake-up is lost when the subscriber sleeps
/// without having seen the sample and the publisher doesn't ring.
fn one_step(recheck: bool, fenced: bool) {
    let mem = Model(Arc::new([AtomicU64::new(0), AtomicU64::new(0)]));
    let publisher = {
        let mem = mem.clone();
        loom::thread::spawn(move || {
            mem.store(HEAD, 1, Ordering::Release);
            if fenced {
                wants_ring(&mem, &mut 0)
            } else {
                // A publisher that reads the page without the fence.
                mem.load(0, Ordering::Acquire) & 1 == 1
            }
        })
    };
    let mut seen = mem.load(HEAD, Ordering::Acquire);
    let mut sleeps = false;
    if seen == 0 {
        arm(&mem, 1);
        if recheck {
            seen = mem.load(HEAD, Ordering::Acquire);
        }
        sleeps = seen == 0;
    }
    let rang = publisher.join().expect("publisher");
    assert!(!sleeps || rang, "lost wake-up");
}

#[test]
fn asleep_without_the_sample_means_the_publisher_rang() {
    loom::model(|| one_step(true, true));
}

/// The model has teeth. Without the second look, a sample published between the first look and
/// the arm is slept through.
#[test]
#[should_panic(expected = "lost wake-up")]
fn sleeping_without_looking_again_loses_a_wake_up() {
    loom::model(|| one_step(false, true));
}

/// And without the publisher's fence, each side can miss the other's write.
#[test]
#[should_panic(expected = "lost wake-up")]
fn a_publisher_that_does_not_fence_loses_a_wake_up() {
    loom::model(|| one_step(true, false));
}
