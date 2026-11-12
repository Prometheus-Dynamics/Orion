//! What the ring costs. Run it on the board the numbers are for:
//!
//! ```text
//! orion-ring-bench [--frames N] [--hz N] [--samples N] [--cpus A,B]
//! ```
//!
//! It prints a report. Times are wall-clock from one monotonic clock; CPU is each thread's own
//! CPU time. The two sides of every measurement run on two different cores.

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use orion_measure::CountingAlloc;
use orion_measure::harness::steady_allocs;
use orion_measure::sched::{SystemCalls, pin_current_thread, thread_cpu_ns, without_system_calls};
use orion_ring::{FieldId, Flags, Next, Reader, Ring, SampleTime, Spec, Writer};
use orion_ring_buffers::{BufferReader, BufferWriter, NextBatch, Pool};
use orion_ring_doorbell::{Bell, Doorbell};
use orion_ring_holds::{HoldTable, Limits, Marks};
use orion_ring_time::at_or_before;

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc::system();

const FIELD: FieldId = FieldId(0);
const PAYLOAD: [u8; 64] = [7; 64];

struct Options {
    frames: u64,
    hz: u64,
    samples: u64,
    cpus: (usize, usize),
}

fn options() -> Options {
    let mut options = Options {
        frames: 1200,
        hz: 120,
        samples: 2000,
        cpus: (2, 3),
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let value = args.next().unwrap_or_default();
        let number = || value.parse::<u64>().expect("a number");
        match arg.as_str() {
            "--frames" => options.frames = number(),
            "--hz" => options.hz = number().max(1),
            "--samples" => options.samples = number(),
            "--cpus" => {
                let (a, b) = value.split_once(',').expect("--cpus A,B");
                options.cpus = (a.parse().expect("a cpu"), b.parse().expect("a cpu"));
            }
            other => panic!("unknown option {other}"),
        }
    }
    options
}

/// Sorted durations in nanoseconds.
struct Spread(Vec<u64>);

impl Spread {
    fn of(mut nanos: Vec<u64>) -> Self {
        nanos.sort_unstable();
        Self(nanos)
    }

    fn at(&self, percent: usize) -> f64 {
        if self.0.is_empty() {
            return f64::NAN;
        }
        let index = (self.0.len() * percent / 100).min(self.0.len() - 1);
        self.0[index] as f64 / 1000.0
    }

    fn line(&self) -> String {
        format!(
            "median {:.2} us, 90% {:.2} us, 99% {:.2} us, worst {:.2} us ({} samples)",
            self.at(50),
            self.at(90),
            self.at(99),
            self.at(100),
            self.0.len()
        )
    }
}

fn value_ring() -> Ring {
    Ring::create(&Spec {
        fields: 1,
        history: 64,
        max_payload: 64,
        expiry_ms: 0,
    })
    .expect("ring")
}

fn publish(writer: &mut Writer, time: u64) {
    writer
        .publish(FIELD, SampleTime::from_nanos(time), Flags::NONE, &PAYLOAD)
        .expect("publish");
}

/// Nanoseconds per call of `body`, over `rounds` calls.
fn per_call(rounds: u64, mut body: impl FnMut(u64)) -> f64 {
    for round in 0..rounds / 10 {
        body(round);
    }
    let started = Instant::now();
    for round in 0..rounds {
        body(round);
    }
    started.elapsed().as_nanos() as f64 / rounds as f64
}

/// The single-thread costs: a publish, a publish with a subscriber that is awake, and reads.
fn memory_paths() {
    const ROUNDS: u64 = 2_000_000;
    println!("## Value topic, memory paths (64-byte payload, one thread)");
    println!();

    let ring = value_ring();
    let mut writer = ring.writer().expect("writer");
    let mut time = 0;
    let nobody = per_call(ROUNDS, |_| {
        time += 1;
        publish(&mut writer, time);
    });
    println!("publish, nobody subscribed: {nobody:.1} ns");

    let doorbell = Doorbell::create().expect("doorbell");
    let mut bell = doorbell.bell().expect("bell");
    let awake = per_call(ROUNDS, |_| {
        time += 1;
        publish(&mut writer, time);
        black_box(bell.ring());
    });
    println!("publish and ring, subscriber awake: {awake:.1} ns");

    let mut reader = ring.reader().expect("reader");
    let mut read = 0_u64;
    let pair = per_call(ROUNDS, |_| {
        time += 1;
        publish(&mut writer, time);
        if let Next::Sample(sample) = reader.next() {
            read += sample.payload.len() as u64;
        }
    });
    println!("publish and next: {pair:.1} ns ({read} bytes read)");
    let latest = per_call(ROUNDS, |_| {
        black_box(reader.latest(FIELD).map(|sample| sample.seq));
    });
    println!("latest: {latest:.1} ns");
    let lookup = per_call(ROUNDS, |round| {
        black_box(
            at_or_before(
                &mut reader,
                FIELD,
                SampleTime::from_nanos(time - round % 60),
            )
            .is_hit(),
        );
    });
    println!("lookup by sample time, 64 deep: {lookup:.1} ns");

    let allocs = steady_allocs(16, 100_000, || {
        time += 1;
        publish(&mut writer, time);
        black_box(bell.ring());
        black_box(matches!(reader.next(), Next::Sample(_)));
    });
    println!(
        "allocations in 100,000 publishes, rings and reads: {}",
        allocs.allocs
    );

    // The same publish and ring in a child that is killed on any system call.
    let calls = without_system_calls(|| {
        for round in 0..100_000_u64 {
            if writer
                .publish(FIELD, SampleTime::from_nanos(round), Flags::NONE, &PAYLOAD)
                .is_err()
                || bell.ring()
            {
                return false;
            }
        }
        true
    });
    println!(
        "system calls in 100,000 publishes with the subscriber awake: {}",
        match calls {
            SystemCalls::None => "none (the child ran under a filter that kills on any)",
            SystemCalls::Made => "MADE ONE",
            SystemCalls::Failed => "could not be checked",
        }
    );
    println!();
}

/// Sets a flag when dropped, so the other thread stops even if this one panics.
struct Stop<'a>(&'a AtomicBool);

impl Drop for Stop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// Waits until the subscriber has taken all `sent` samples and is asleep on its doorbell again.
/// `false` if it doesn't get there, which would be a lost wake-up.
fn until_asleep(bell: &Bell, taken: &AtomicU64, sent: u64) -> bool {
    let started = Instant::now();
    while taken.load(Ordering::Acquire) != sent || !bell.asleep() {
        if started.elapsed() > Duration::from_secs(5) {
            return false;
        }
        std::hint::spin_loop();
    }
    // Asleep in the page is just before asleep in the kernel. Give it time to get there, so
    // the wake that is measured is a real one.
    let settle = Instant::now();
    while settle.elapsed() < Duration::from_micros(300) {
        std::hint::spin_loop();
    }
    true
}

/// A value published to a subscriber asleep on its doorbell.
fn value_wake(options: &Options) {
    println!(
        "## Value topic, subscriber asleep ({} samples)",
        options.samples
    );
    println!();
    let ring = value_ring();
    let mut writer = ring.writer().expect("writer");
    let mut reader = ring.reader().expect("reader");
    let mut doorbell = Doorbell::create().expect("doorbell");
    let mut bell = doorbell.bell().expect("bell");
    let base = Instant::now();
    let done = AtomicBool::new(false);
    let taken = AtomicU64::new(0);
    let subscriber_cpu = AtomicU64::new(0);
    let samples = options.samples;

    let (publish_cost, latency, publisher_cpu, stalls) = std::thread::scope(|scope| {
        let subscriber = scope.spawn(|| {
            pin_current_thread(options.cpus.1);
            let started = thread_cpu_ns();
            let mut latency = Vec::with_capacity(samples as usize);
            while !done.load(Ordering::Acquire) {
                let mut got = None;
                let _ = doorbell.wait(Some(Duration::from_millis(200)), || {
                    read_one(&mut reader, &mut got);
                    got.is_some()
                });
                read_one(&mut reader, &mut got);
                if let Some(sent) = got {
                    latency.push((base.elapsed().as_nanos() as u64).saturating_sub(sent));
                    taken.fetch_add(1, Ordering::Release);
                }
            }
            subscriber_cpu.store(thread_cpu_ns() - started, Ordering::Release);
            latency
        });
        let stop = Stop(&done);
        pin_current_thread(options.cpus.0);
        let mut cost = Vec::with_capacity(samples as usize);
        let (mut cpu, mut stalls) = (0, 0_u64);
        for sample in 0..samples {
            if !until_asleep(&bell, &taken, sample) {
                stalls += 1;
                break;
            }
            let cpu_before = thread_cpu_ns();
            let sent = base.elapsed().as_nanos() as u64;
            publish(&mut writer, sent);
            let rang = bell.ring();
            cost.push(base.elapsed().as_nanos() as u64 - sent);
            cpu += thread_cpu_ns() - cpu_before;
            stalls += u64::from(!rang);
        }
        std::thread::sleep(Duration::from_millis(5));
        drop(stop);
        (cost, subscriber.join().expect("subscriber"), cpu, stalls)
    });
    println!(
        "samples published {}, received {}, publishes that found the subscriber not asleep {stalls}",
        publish_cost.len(),
        latency.len()
    );
    println!(
        "publish and ring (one write): {}",
        Spread::of(publish_cost).line()
    );
    println!(
        "publish to subscriber running with the sample: {}",
        Spread::of(latency).line()
    );
    println!(
        "CPU per sample: publisher {:.2} us, subscriber {:.2} us",
        publisher_cpu as f64 / samples as f64 / 1000.0,
        subscriber_cpu.load(Ordering::Acquire) as f64 / samples as f64 / 1000.0
    );
    println!();
}

fn read_one(reader: &mut Reader, got: &mut Option<u64>) {
    if got.is_none()
        && let Next::Sample(sample) = reader.next()
    {
        *got = Some(sample.time.as_nanos());
    }
}

/// A frame topic at a fixed rate, with hold marks and a subscriber that sleeps between frames.
fn frames(options: &Options) {
    const BUFFERS: u32 = 4;
    const FRAME: usize = 1 << 20;
    println!(
        "## Buffer topic with hold marks ({} frames at {} Hz, {BUFFERS} buffers of {FRAME} bytes)",
        options.frames, options.hz
    );
    println!();
    let ring = Ring::create(&Spec {
        fields: 1,
        history: 8,
        max_payload: 64,
        expiry_ms: 0,
    })
    .expect("ring");
    let pool = Pool::in_memory(1, BUFFERS, FRAME).expect("pool");
    let view = pool.view().expect("view");
    let limits = Limits {
        max_held: 2,
        max_hold: Some(Duration::from_secs(2)),
    };
    let marks = Marks::create(BUFFERS, 0, limits.max_held).expect("marks");
    let mut table = HoldTable::new(BUFFERS);
    table
        .add(0, marks.page_fd().expect("marks page"), limits)
        .expect("reader");
    let mut writer = BufferWriter::new(ring.writer().expect("writer"), FIELD, pool);
    let mut reader = BufferReader::new(ring.reader().expect("reader"), FIELD, view, marks);
    let mut doorbell = Doorbell::create().expect("doorbell");
    let mut bell = doorbell.bell().expect("bell");
    let base = Instant::now();
    let done = AtomicBool::new(false);
    let subscriber_cpu = AtomicU64::new(0);
    let period = Duration::from_nanos(1_000_000_000 / options.hz);
    let frames = options.frames;

    let (publish_cost, latency, publisher_cpu, dropped, rings) = std::thread::scope(|scope| {
        let subscriber = scope.spawn(|| {
            pin_current_thread(options.cpus.1);
            let started = thread_cpu_ns();
            let mut latency = Vec::with_capacity(frames as usize);
            let mut pending = None;
            while !done.load(Ordering::Acquire) {
                let _ = doorbell.wait(Some(Duration::from_millis(200)), || {
                    take_frame(&mut reader, &mut pending);
                    pending.is_some()
                });
                take_frame(&mut reader, &mut pending);
                if let Some((sent, held)) = pending.take() {
                    // The frame is held: read the number the publisher wrote in the buffer,
                    // in place, as a consumer of the frame would start to.
                    let bytes = held.bytes(0).expect("bytes");
                    assert_eq!(bytes[..8], sent.to_le_bytes(), "the frame's own bytes");
                    latency.push((base.elapsed().as_nanos() as u64).saturating_sub(sent));
                    drop(held);
                }
            }
            subscriber_cpu.store(thread_cpu_ns() - started, Ordering::Release);
            latency
        });
        let stop = Stop(&done);
        pin_current_thread(options.cpus.0);
        let mut cost = Vec::with_capacity(frames as usize);
        let (mut cpu, mut dropped, mut rings) = (0, 0_u64, 0_u64);
        let mut next = Instant::now() + period;
        for _ in 0..frames {
            std::thread::sleep(next.saturating_duration_since(Instant::now()));
            next += period;
            // What a camera driver does is not Orion's cost: the buffer is filled (here, just
            // stamped) before the clock starts, as a captured frame arrives already filled.
            let now = base.elapsed().as_nanos() as u64;
            table.enforce(now, writer.pool().page());
            let Ok(buffer) = writer.take(&table) else {
                dropped += 1;
                continue;
            };
            let cpu_before = thread_cpu_ns();
            let sent = base.elapsed().as_nanos() as u64;
            writer.buffer_mut(buffer).expect("free")[..8].copy_from_slice(&sent.to_le_bytes());
            table.enforce(sent, writer.pool().page());
            writer
                .publish(&[buffer], SampleTime::from_nanos(sent), &PAYLOAD[..16])
                .expect("publish");
            rings += u64::from(bell.ring());
            cost.push(base.elapsed().as_nanos() as u64 - sent);
            cpu += thread_cpu_ns() - cpu_before;
        }
        std::thread::sleep(Duration::from_millis(20));
        drop(stop);
        (
            cost,
            subscriber.join().expect("subscriber"),
            cpu,
            dropped,
            rings,
        )
    });
    let received = latency.len() as u64;
    println!(
        "frames published {}, dropped {dropped}, received {received}",
        publish_cost.len()
    );
    println!("the subscriber was asleep and was rung for {rings} of them");
    println!(
        "publisher, per frame (stamp, limits, entry, ring): {}",
        Spread::of(publish_cost).line()
    );
    println!(
        "publish to subscriber holding the frame and reading it: {}",
        Spread::of(latency).line()
    );
    println!(
        "CPU per frame: publisher {:.2} us, subscriber {:.2} us",
        publisher_cpu as f64 / frames as f64 / 1000.0,
        subscriber_cpu.load(Ordering::Acquire) as f64 / frames as f64 / 1000.0
    );
    println!();
}

type HeldFrame = orion_ring_buffers::Held<Marks>;

fn take_frame(
    reader: &mut BufferReader<orion_ring::ReadMap, Marks>,
    pending: &mut Option<(u64, HeldFrame)>,
) {
    if pending.is_none()
        && let NextBatch::Batch(frame) = reader.next()
    {
        *pending = Some((frame.time.as_nanos(), frame.held));
    }
}

/// Frames with the subscriber awake: publishing a frame is then memory writes only.
fn frames_awake() {
    const BUFFERS: u32 = 4;
    const ROUNDS: u64 = 1_000_000;
    println!("## Buffer topic with hold marks, subscriber awake (one thread)");
    println!();
    let ring = Ring::create(&Spec {
        fields: 1,
        history: 8,
        max_payload: 64,
        expiry_ms: 0,
    })
    .expect("ring");
    let pool = Pool::in_memory(1, BUFFERS, 4096).expect("pool");
    let view = pool.view().expect("view");
    let marks = Marks::create(BUFFERS, 0, 2).expect("marks");
    let mut table = HoldTable::new(BUFFERS);
    let limits = Limits {
        max_held: 2,
        max_hold: Some(Duration::from_secs(2)),
    };
    table
        .add(0, marks.page_fd().expect("marks page"), limits)
        .expect("reader");
    let mut writer = BufferWriter::new(ring.writer().expect("writer"), FIELD, pool);
    let mut reader = BufferReader::new(ring.reader().expect("reader"), FIELD, view, marks);
    let doorbell = Doorbell::create().expect("doorbell");
    let mut bell = doorbell.bell().expect("bell");

    let mut time = 0_u64;
    let mut held = 0_u64;
    let mut frame = |writer: &mut BufferWriter, table: &mut HoldTable, bell: &mut Bell| {
        time += 1;
        table.enforce(time, writer.pool().page());
        let Ok(buffer) = writer.take(&*table) else {
            return false;
        };
        if writer
            .publish(&[buffer], SampleTime::from_nanos(time), &PAYLOAD[..16])
            .is_err()
            || bell.ring()
        {
            return false;
        }
        if let NextBatch::Batch(frame) = reader.next() {
            held += u64::from(frame.held.bytes(0).is_some());
        }
        true
    };
    let each = per_call(ROUNDS, |_| {
        black_box(frame(&mut writer, &mut table, &mut bell));
    });
    println!("limits, take, publish, ring, next with marks, release: {each:.1} ns");
    let allocs = steady_allocs(16, 100_000, || {
        black_box(frame(&mut writer, &mut table, &mut bell));
    });
    println!("allocations in 100,000 frames: {}", allocs.allocs);
    let calls =
        without_system_calls(|| (0..100_000).all(|_| frame(&mut writer, &mut table, &mut bell)));
    println!(
        "system calls in 100,000 frames, both sides: {}",
        match calls {
            SystemCalls::None => "none (the child ran under a filter that kills on any)",
            SystemCalls::Made => "MADE ONE",
            SystemCalls::Failed => "could not be checked",
        }
    );
    println!();
}

fn main() {
    let options = options();
    println!("# Ring costs");
    println!();
    println!(
        "publisher on cpu {}, subscriber on cpu {}",
        options.cpus.0, options.cpus.1
    );
    println!();
    pin_current_thread(options.cpus.0);
    memory_paths();
    frames_awake();
    value_wake(&options);
    frames(&options);
}
