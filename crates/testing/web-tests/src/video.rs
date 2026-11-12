//! A publisher of the video example's test clip: JPEG pictures on one topic and fragments of
//! fragmented MP4 on another, in a loop. Orion is given bytes and one bit, "others depend on
//! this one". What the bytes are is this file's business and the page's.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use orion::topics::{FieldId, Flags, SampleTime, TopicDecl};
use orion::{Error, Orion, fields};

/// The topic of JPEG pictures.
pub const JPEG: &str = "camera.jpeg";
/// The topic of H.264 fragments.
pub const H264: &str = "camera.h264";

/// One sample of the H.264 topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fragment {
    /// Whether it starts a group of pictures. If so it begins with the stream's header.
    pub key: bool,
    /// The bytes: one `moof` and its `mdat`, after the header if `key`.
    pub bytes: Vec<u8>,
}

/// The test clip, in both forms.
#[derive(Debug, Clone, Default)]
pub struct Clip {
    /// The pictures, in order.
    pub jpegs: Vec<Vec<u8>>,
    /// The fragments, in order.
    pub fragments: Vec<Fragment>,
}

/// The boxes at the top of an MP4 file: each one's kind and all its bytes.
fn boxes(file: &[u8]) -> Vec<(&[u8], &[u8])> {
    let mut found = Vec::new();
    let mut rest = file;
    while let Some((head, _)) = rest.split_first_chunk::<8>() {
        let size = u32::from_be_bytes([head[0], head[1], head[2], head[3]]) as usize;
        if size < 8 || size > rest.len() {
            break;
        }
        let (whole, after) = rest.split_at(size);
        found.push((&whole[4..8], whole));
        rest = after;
    }
    found
}

/// Whether an `mdat` holds a picture that needs no other: an IDR slice among its units,
/// each of which is its length in four bytes and then itself.
fn has_key_picture(mdat: &[u8]) -> bool {
    let mut rest = mdat.get(8..).unwrap_or_default();
    while let Some((length, after)) = rest.split_first_chunk::<4>() {
        let length = u32::from_be_bytes(*length) as usize;
        let Some(unit) = after.get(..length) else {
            break;
        };
        if unit.first().is_some_and(|first| first & 0x1f == 5) {
            return true;
        }
        rest = &after[length..];
    }
    false
}

/// Cuts a fragmented MP4 file into samples: one for each `moof` and its `mdat`, with the
/// header (`ftyp` and `moov`) at the front of each one that starts a group of pictures.
#[must_use]
pub fn fragments(file: &[u8]) -> Vec<Fragment> {
    let mut header = Vec::new();
    let mut found = Vec::new();
    let mut moof: Option<&[u8]> = None;
    for (kind, whole) in boxes(file) {
        match kind {
            b"ftyp" | b"moov" => header.extend_from_slice(whole),
            b"moof" => moof = Some(whole),
            b"mdat" => {
                if let Some(moof) = moof.take() {
                    let key = has_key_picture(whole);
                    let mut bytes = if key { header.clone() } else { Vec::new() };
                    bytes.extend_from_slice(moof);
                    bytes.extend_from_slice(whole);
                    found.push(Fragment { key, bytes });
                }
            }
            _ => {}
        }
    }
    found
}

impl Clip {
    /// Reads `frame-NN.jpg` and `clip.mp4` from `dir`.
    ///
    /// # Errors
    ///
    /// If the directory cannot be read.
    pub fn load(dir: &Path) -> std::io::Result<Self> {
        let mut names: Vec<_> = std::fs::read_dir(dir)?
            .filter_map(|entry| Some(entry.ok()?.path()))
            .filter(|path| path.extension().is_some_and(|ext| ext == "jpg"))
            .collect();
        names.sort();
        let jpegs = names
            .iter()
            .map(std::fs::read)
            .collect::<std::io::Result<Vec<_>>>()?;
        let fragments = fragments(&std::fs::read(dir.join("clip.mp4"))?);
        Ok(Self { jpegs, fragments })
    }
}

/// Publishes the clip in a loop, `rate` samples a second on each topic, until `stop`. Each
/// sample's time is its number, counting from one, so a reader can tell what it was not sent.
///
/// # Errors
///
/// If the topics cannot be declared or the connection breaks.
pub fn publish(orion: &Orion, clip: &Clip, rate: u32, stop: &AtomicBool) -> Result<(), Error> {
    let largest = |sizes: &mut dyn Iterator<Item = usize>| sizes.max().unwrap_or(0) as u32;
    let mut jpeg = orion.declare(&TopicDecl {
        name: JPEG,
        fields: &fields![picture: Bytes],
        history: 8,
        expiry_ms: 0,
        max_payload: largest(&mut clip.jpegs.iter().map(Vec::len)),
    })?;
    let mut h264 = orion.declare(&TopicDecl {
        name: H264,
        fields: &fields![fragment: Bytes],
        history: 32,
        expiry_ms: 0,
        max_payload: largest(&mut clip.fragments.iter().map(|fragment| fragment.bytes.len())),
    })?;
    let between = Duration::from_secs(1) / rate.max(1);
    let mut number = 0_u64;
    while !stop.load(Ordering::Relaxed) {
        let at = number as usize;
        number += 1;
        let time = SampleTime::from_nanos(number);
        if !clip.jpegs.is_empty() {
            jpeg.publish(FieldId(0), time, &clip.jpegs[at % clip.jpegs.len()])?;
        }
        if !clip.fragments.is_empty() {
            let fragment = &clip.fragments[at % clip.fragments.len()];
            let flags = if fragment.key {
                Flags::OTHERS_DEPEND
            } else {
                Flags::NONE
            };
            h264.publish_flagged(FieldId(0), time, flags, &fragment.bytes)?;
        }
        orion.pump(between)?;
    }
    Ok(())
}
