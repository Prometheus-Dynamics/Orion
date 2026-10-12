//! Pairs camera results with the flips that caused them, and computes the three latency numbers.
//!
//! Every time here is microseconds on the tool's clock, which is the NT server clock PhotonVision
//! syncs to. A flip's `painted_us` is the page's paint time converted to that clock. A result's
//! `capture_us` and `publish_us` are already on it (PhotonVision adds its NT time offset before
//! publishing). `arrival_us` is when the tool received the value.
//!
//! * a, true latency: `arrival - painted`.
//! * b, reported latency: `arrival - capture`.
//! * c, timestamp error: `capture - painted - display_delay`. Zero means the capture timestamp is
//!   exact once the display delay is taken out.

use std::fmt::Write as _;
use std::io::{self, Write};

/// One commanded flip, as the page confirmed it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Flip {
    pub seq: u32,
    /// Tag shown (true) or black screen (false).
    pub on: bool,
    /// When the tool sent the command, on the tool clock.
    pub cmd_us: i64,
    /// When the page painted it, on the tool clock. Equals `cmd_us` until the page reports.
    pub painted_us: i64,
    /// False when the page never reported the paint, so `painted_us` is the command time.
    pub painted_confirmed: bool,
}

/// A `hasTarget` value as the tool received it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HasTargetSample {
    pub arrival_us: i64,
    pub has_target: bool,
}

/// The metadata at the start of a PhotonPipelineResult (`rawBytes`). Times are microseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PhotonMetadata {
    pub sequence_id: i64,
    pub capture_us: i64,
    pub publish_us: i64,
    /// Passed through as published; PhotonVision's own ping age, not used here.
    pub time_since_last_pong: i64,
}

/// A decoded `rawBytes` value and when the tool received it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResultSample {
    pub arrival_us: i64,
    pub meta: PhotonMetadata,
}

/// Decodes the metadata at the start of a `rawBytes` value.
///
/// PhotonVision packs `PhotonPipelineResult` with WPILib-style little-endian fields
/// (`PhotonPipelineMetadataSerde` in photonvision's `photon-targeting`). The first 32 bytes are four
/// int64 values in order: sequenceID, captureTimestampMicros, publishTimestampMicros,
/// timeSinceLastPong. The targets follow and are not needed for timing.
pub fn decode_metadata(raw: &[u8]) -> Option<PhotonMetadata> {
    let field = |index: usize| -> Option<i64> {
        let bytes: [u8; 8] = raw.get(index * 8..index * 8 + 8)?.try_into().ok()?;
        Some(i64::from_le_bytes(bytes))
    };
    Some(PhotonMetadata {
        sequence_id: field(0)?,
        capture_us: field(1)?,
        publish_us: field(2)?,
        time_since_last_pong: field(3)?,
    })
}

/// Tolerances and the display delay.
#[derive(Clone, Copy, Debug)]
pub struct MatchConfig {
    /// Time from the page's paint to the light leaving the screen. Measure once, see the README.
    pub display_delay_us: i64,
    /// A result pairs with a `hasTarget` transition if they arrive this close together.
    pub pair_window_us: i64,
    /// A transition only matches a flip painted at most this long before the frame's capture.
    pub max_match_us: i64,
    /// A result whose publish time is further than this from its arrival is not on the tool's
    /// clock (PhotonVision has not synced yet). Such results are excluded and counted.
    pub max_clock_error_us: i64,
}

impl MatchConfig {
    pub fn new(display_delay_us: i64) -> Self {
        Self {
            display_delay_us,
            pair_window_us: 100_000,
            max_match_us: 1_000_000,
            max_clock_error_us: 1_000_000,
        }
    }
}

/// The three measured numbers for one matched flip, in microseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Match {
    /// Arrival of the `hasTarget` value that shows the flip.
    pub nt_arrival_us: i64,
    pub capture_us: i64,
    pub publish_us: i64,
    /// a
    pub true_latency_us: i64,
    /// b
    pub reported_latency_us: i64,
    /// c
    pub timestamp_error_us: i64,
}

/// A flip and the match found for it, if any.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Row {
    pub flip: Flip,
    pub matched: Option<Match>,
}

/// Counts of everything the matcher saw. Drops are flips with no matched transition.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    pub flips: usize,
    pub matched: usize,
    pub dropped: usize,
    /// Changes of `hasTarget` (the first value only sets the baseline).
    pub transitions: usize,
    /// No result arrived within the pair window, so there is no capture time.
    pub unpaired_transitions: usize,
    /// No flip was painted at or before the capture, or it was too old.
    pub orphan_transitions: usize,
    /// The nearest preceding flip shows the other state.
    pub mismatched_transitions: usize,
    /// The nearest preceding flip was already matched by an earlier transition.
    pub duplicate_transitions: usize,
    /// Results excluded because their clock is not synced to the tool.
    pub unsynced_results: usize,
}

/// Percentiles of one metric, in milliseconds (nearest-rank).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Percentiles {
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub max: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Analysis {
    pub rows: Vec<Row>,
    pub counts: Counts,
    pub true_latency: Option<Percentiles>,
    pub reported_latency: Option<Percentiles>,
    pub timestamp_error: Option<Percentiles>,
}

/// Matches camera transitions to flips and computes the three metrics.
pub fn analyze(
    flips: &[Flip],
    has: &[HasTargetSample],
    results: &[ResultSample],
    cfg: &MatchConfig,
) -> Analysis {
    let mut counts = Counts {
        flips: flips.len(),
        ..Counts::default()
    };
    let mut rows: Vec<Row> = flips
        .iter()
        .map(|&flip| Row {
            flip,
            matched: None,
        })
        .collect();

    let mut synced: Vec<ResultSample> = Vec::new();
    for result in results {
        let clock_error = result.meta.publish_us - result.arrival_us;
        if clock_error.abs() <= cfg.max_clock_error_us {
            synced.push(*result);
        } else {
            counts.unsynced_results += 1;
        }
    }
    synced.sort_by_key(|r| r.arrival_us);

    // Row indices ordered by painted time, for "nearest preceding flip".
    let mut by_paint: Vec<usize> = (0..rows.len()).collect();
    by_paint.sort_by_key(|&i| rows[i].flip.painted_us);

    for (arrival_us, has_target) in transitions(has) {
        counts.transitions += 1;
        let Some(result) = nearest(&synced, arrival_us, cfg.pair_window_us) else {
            counts.unpaired_transitions += 1;
            continue;
        };
        let capture_us = result.meta.capture_us;
        let k = by_paint.partition_point(|&i| rows[i].flip.painted_us <= capture_us);
        if k == 0 {
            counts.orphan_transitions += 1;
            continue;
        }
        let index = by_paint[k - 1];
        let flip = rows[index].flip;
        if capture_us - flip.painted_us > cfg.max_match_us {
            counts.orphan_transitions += 1;
            continue;
        }
        if flip.on != has_target {
            counts.mismatched_transitions += 1;
            continue;
        }
        if rows[index].matched.is_some() {
            counts.duplicate_transitions += 1;
            continue;
        }
        rows[index].matched = Some(Match {
            nt_arrival_us: arrival_us,
            capture_us,
            publish_us: result.meta.publish_us,
            true_latency_us: arrival_us - flip.painted_us,
            reported_latency_us: arrival_us - capture_us,
            timestamp_error_us: capture_us - flip.painted_us - cfg.display_delay_us,
        });
    }

    counts.matched = rows.iter().filter(|r| r.matched.is_some()).count();
    counts.dropped = counts.flips - counts.matched;
    let metric = |pick: fn(&Match) -> i64| {
        percentiles(
            rows.iter()
                .filter_map(|r| r.matched.as_ref())
                .map(|m| to_ms(pick(m)))
                .collect(),
        )
    };
    Analysis {
        true_latency: metric(|m| m.true_latency_us),
        reported_latency: metric(|m| m.reported_latency_us),
        timestamp_error: metric(|m| m.timestamp_error_us),
        rows,
        counts,
    }
}

/// `hasTarget` changes as (arrival, new state). The first sample is the baseline.
fn transitions(has: &[HasTargetSample]) -> Vec<(i64, bool)> {
    let mut sorted = has.to_vec();
    sorted.sort_by_key(|s| s.arrival_us);
    let mut out = Vec::new();
    let mut previous: Option<bool> = None;
    for sample in sorted {
        if let Some(p) = previous
            && p != sample.has_target
        {
            out.push((sample.arrival_us, sample.has_target));
        }
        previous = Some(sample.has_target);
    }
    out
}

/// The synced result whose arrival is closest to `arrival_us`, within `window_us`.
fn nearest(synced: &[ResultSample], arrival_us: i64, window_us: i64) -> Option<ResultSample> {
    let k = synced.partition_point(|r| r.arrival_us < arrival_us);
    [k.checked_sub(1), (k < synced.len()).then_some(k)]
        .into_iter()
        .flatten()
        .map(|i| synced[i])
        .min_by_key(|r| (r.arrival_us - arrival_us).abs())
        .filter(|r| (r.arrival_us - arrival_us).abs() <= window_us)
}

fn to_ms(us: i64) -> f64 {
    us as f64 / 1000.0
}

/// Nearest-rank percentiles of `values_ms`, or `None` when empty.
pub fn percentiles(mut values_ms: Vec<f64>) -> Option<Percentiles> {
    if values_ms.is_empty() {
        return None;
    }
    values_ms.sort_by(f64::total_cmp);
    let n = values_ms.len();
    let at = |p: f64| {
        let rank = ((p / 100.0) * n as f64).ceil() as usize;
        values_ms[rank.clamp(1, n) - 1]
    };
    Some(Percentiles {
        p50: at(50.0),
        p90: at(90.0),
        p99: at(99.0),
        max: values_ms[n - 1],
    })
}

/// The per-flip CSV. Dropped flips have an empty measurement.
pub fn write_csv(out: &mut impl Write, rows: &[Row]) -> io::Result<()> {
    writeln!(
        out,
        "seq,tag_on,status,flip_cmd_us,flip_painted_us,painted_confirmed,capture_us,publish_us,\
         nt_arrival_us,true_latency_ms,reported_latency_ms,timestamp_error_ms"
    )?;
    for row in rows {
        let f = row.flip;
        match row.matched {
            Some(m) => writeln!(
                out,
                "{},{},matched,{},{},{},{},{},{},{:.3},{:.3},{:.3}",
                f.seq,
                f.on,
                f.cmd_us,
                f.painted_us,
                f.painted_confirmed,
                m.capture_us,
                m.publish_us,
                m.nt_arrival_us,
                to_ms(m.true_latency_us),
                to_ms(m.reported_latency_us),
                to_ms(m.timestamp_error_us),
            )?,
            None => writeln!(
                out,
                "{},{},dropped,{},{},{},,,,,,",
                f.seq, f.on, f.cmd_us, f.painted_us, f.painted_confirmed,
            )?,
        }
    }
    Ok(())
}

/// The human summary: counts, then p50/p90/p99/max for a, b and c.
pub fn format_summary(analysis: &Analysis) -> String {
    let c = &analysis.counts;
    let mut s = String::new();
    let _ = writeln!(
        s,
        "flips: {}  matched: {}  dropped: {}",
        c.flips, c.matched, c.dropped
    );
    let _ = writeln!(
        s,
        "hasTarget transitions: {}  unpaired: {}  orphan: {}  mismatched: {}  duplicate: {}  \
         results off the tool clock: {}",
        c.transitions,
        c.unpaired_transitions,
        c.orphan_transitions,
        c.mismatched_transitions,
        c.duplicate_transitions,
        c.unsynced_results,
    );
    let lines = [
        ("a true latency (flip -> NT arrival)", analysis.true_latency),
        (
            "b reported latency (capture -> NT arrival)",
            analysis.reported_latency,
        ),
        (
            "c timestamp error (capture - flip - display delay)",
            analysis.timestamp_error,
        ),
    ];
    for (name, p) in lines {
        match p {
            Some(p) => {
                let _ = writeln!(
                    s,
                    "{name:<54} p50 {:8.3}  p90 {:8.3}  p99 {:8.3}  max {:8.3} ms",
                    p.p50, p.p90, p.p99, p.max
                );
            }
            None => {
                let _ = writeln!(s, "{name:<54} no matched flips");
            }
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    const DELAY: i64 = 20_000;

    fn flip(seq: u32, on: bool, painted_us: i64) -> Flip {
        Flip {
            seq,
            on,
            cmd_us: painted_us - 1_000,
            painted_us,
            painted_confirmed: true,
        }
    }

    /// A result published `publish_lag` after `capture` and received at once.
    fn result(capture_us: i64, publish_lag_us: i64) -> ResultSample {
        let publish_us = capture_us + publish_lag_us;
        ResultSample {
            arrival_us: publish_us + 500,
            meta: PhotonMetadata {
                sequence_id: 0,
                capture_us,
                publish_us,
                time_since_last_pong: 0,
            },
        }
    }

    /// Camera transitions as the hasTarget values that show them, one per frame.
    fn has_samples(frames: &[(i64, bool)]) -> Vec<HasTargetSample> {
        frames
            .iter()
            .map(|&(t, has)| HasTargetSample {
                arrival_us: t,
                has_target: has,
            })
            .collect()
    }

    fn cfg() -> MatchConfig {
        MatchConfig::new(DELAY)
    }

    #[test]
    fn metadata_decodes_little_endian_fields() {
        let mut raw = Vec::new();
        for v in [7i64, 1_000_000, 1_000_400, 33] {
            raw.extend_from_slice(&v.to_le_bytes());
        }
        raw.extend_from_slice(&[9, 9, 9]); // targets follow; ignored
        let meta = decode_metadata(&raw).unwrap();
        assert_eq!(meta.sequence_id, 7);
        assert_eq!(meta.capture_us, 1_000_000);
        assert_eq!(meta.publish_us, 1_000_400);
        assert_eq!(meta.time_since_last_pong, 33);
        assert!(decode_metadata(&raw[..31]).is_none());
    }

    #[test]
    fn synthetic_timeline_gives_exact_numbers() {
        // Flips every 300 ms. The camera shows each flip 30 ms after the paint (its frame is
        // captured then), publishes 25 ms after capture, and the tool receives it 0.5 ms later.
        let flips: Vec<Flip> = (0..4)
            .map(|i| flip(i, i % 2 == 0, 1_000_000 + i as i64 * 300_000))
            .collect();
        let mut has = vec![];
        let mut results = vec![];
        // Baseline off before the first flip, then one result per frame of 33 ms.
        let lag = 30_000;
        for f in &flips {
            let capture = f.painted_us + lag;
            let r = result(capture, 25_000);
            results.push(r);
            has.push(HasTargetSample {
                arrival_us: r.arrival_us,
                has_target: f.on,
            });
            // a second frame in the same state, which must not count as a transition
            let r2 = result(capture + 33_000, 25_000);
            results.push(r2);
            has.push(HasTargetSample {
                arrival_us: r2.arrival_us,
                has_target: f.on,
            });
        }
        // The first flip is "on", so the baseline must be off: add the pre-flip sample.
        has.insert(
            0,
            HasTargetSample {
                arrival_us: 900_000,
                has_target: false,
            },
        );
        let analysis = analyze(&flips, &has, &results, &cfg());
        assert_eq!(analysis.counts.flips, 4);
        assert_eq!(analysis.counts.matched, 4, "{:?}", analysis.counts);
        assert_eq!(analysis.counts.dropped, 0);
        assert_eq!(analysis.counts.transitions, 4);
        for row in &analysis.rows {
            let m = row.matched.unwrap();
            assert_eq!(m.true_latency_us, 30_000 + 25_000 + 500);
            assert_eq!(m.reported_latency_us, 25_500);
            assert_eq!(m.timestamp_error_us, 30_000 - DELAY);
        }
        let a = analysis.true_latency.unwrap();
        assert_eq!((a.p50, a.max), (55.5, 55.5));
        let c = analysis.timestamp_error.unwrap();
        assert_eq!(c.p50, 10.0);
    }

    #[test]
    fn flips_inside_one_camera_frame_are_dropped() {
        // Flips 1 and 2 land within one camera frame, so the camera never shows them as changes.
        let flips = vec![
            flip(0, true, 1_000_000),
            flip(1, false, 1_300_000),
            flip(2, true, 1_310_000),
            flip(3, false, 1_600_000),
        ];
        let has = has_samples(&[(900_000, false), (1_050_000, true), (1_650_000, false)]);
        let results = vec![result(1_030_000, 25_000), result(1_630_000, 25_000)];
        let analysis = analyze(&flips, &has, &results, &cfg());
        assert_eq!(analysis.counts.matched, 2);
        assert_eq!(analysis.counts.dropped, 2);
        assert!(analysis.rows[0].matched.is_some());
        assert!(analysis.rows[1].matched.is_none());
        assert!(analysis.rows[2].matched.is_none());
        assert!(analysis.rows[3].matched.is_some());
    }

    #[test]
    fn transition_without_nearby_result_is_unpaired() {
        let flips = vec![flip(0, true, 1_000_000)];
        let has = has_samples(&[(900_000, false), (1_050_000, true)]);
        let analysis = analyze(&flips, &has, &[], &cfg());
        assert_eq!(analysis.counts.unpaired_transitions, 1);
        assert_eq!(analysis.counts.dropped, 1);
        assert!(analysis.true_latency.is_none());
    }

    #[test]
    fn result_clock_not_synced_to_the_tool_is_excluded() {
        let flips = vec![flip(0, true, 1_000_000)];
        let has = has_samples(&[(900_000, false), (1_050_000, true)]);
        // Capture in the camera's own clock: publish is seconds away from arrival.
        let unsynced = ResultSample {
            arrival_us: 1_050_000,
            meta: PhotonMetadata {
                sequence_id: 0,
                capture_us: 1_030_000,
                publish_us: 1_030_000 + 9_000_000_000,
                time_since_last_pong: 0,
            },
        };
        let analysis = analyze(&flips, &has, &[unsynced], &cfg());
        assert_eq!(analysis.counts.unsynced_results, 1);
        assert_eq!(analysis.counts.dropped, 1);
    }

    #[test]
    fn transition_pointing_at_the_wrong_state_is_mismatched() {
        // The nearest preceding flip is "off", but the camera goes to "on".
        let flips = vec![flip(0, true, 1_000_000), flip(1, false, 1_300_000)];
        let has = has_samples(&[(1_400_000, false), (1_450_000, true)]);
        let results = vec![result(1_340_000, 25_000)];
        let analysis = analyze(&flips, &has, &results, &cfg());
        assert_eq!(analysis.counts.mismatched_transitions, 1);
        assert_eq!(analysis.counts.matched, 0);
    }

    #[test]
    fn second_transition_for_one_flip_is_a_duplicate() {
        let flips = vec![flip(0, true, 1_000_000)];
        let has = has_samples(&[
            (900_000, false),
            (1_050_000, true),
            (1_080_000, false),
            (1_110_000, true),
        ]);
        let results = vec![
            result(1_030_000, 25_000),
            result(1_060_000, 25_000),
            result(1_090_000, 25_000),
            result(1_120_000, 25_000),
        ];
        let analysis = analyze(&flips, &has, &results, &cfg());
        assert_eq!(analysis.counts.matched, 1);
        assert_eq!(analysis.counts.duplicate_transitions, 1);
        assert_eq!(analysis.counts.mismatched_transitions, 1);
    }

    #[test]
    fn capture_before_any_flip_is_an_orphan() {
        let flips = vec![flip(0, true, 2_000_000)];
        let has = has_samples(&[(900_000, false), (1_050_000, true)]);
        let results = vec![result(1_030_000, 25_000)];
        let analysis = analyze(&flips, &has, &results, &cfg());
        assert_eq!(analysis.counts.orphan_transitions, 1);
    }

    #[test]
    fn percentiles_are_nearest_rank() {
        let values: Vec<f64> = (1..=100).map(f64::from).collect();
        let p = percentiles(values).unwrap();
        assert_eq!((p.p50, p.p90, p.p99, p.max), (50.0, 90.0, 99.0, 100.0));
        assert_eq!(percentiles(vec![4.0]).unwrap().p99, 4.0);
        assert!(percentiles(vec![]).is_none());
    }

    #[test]
    fn csv_has_one_line_per_flip() {
        let flips = vec![flip(0, true, 1_000_000), flip(1, false, 1_300_000)];
        let has = has_samples(&[(900_000, false), (1_050_000, true)]);
        let results = vec![result(1_030_000, 25_000)];
        let analysis = analyze(&flips, &has, &results, &cfg());
        let mut out = Vec::new();
        write_csv(&mut out, &analysis.rows).unwrap();
        let text = String::from_utf8(out).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[1].starts_with("0,true,matched,"));
        assert!(lines[2].starts_with("1,false,dropped,"));
    }
}
