//! Power goes at every step of a save, on a disk that keeps only what was made safe, and the
//! node still starts with a whole snapshot. Files that are not snapshots never stop it.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

use orion_msg_records::RecordClass;
use orion_persist::{Disk, FORMAT, Loaded, Persist, Source};
use orion_store::{Record, Store, StoreConfig, Version};
use orion_types::HlcTimestamp;

/// A file's contents: what a running program sees, and what is safe on the disk.
#[derive(Clone, Debug)]
struct Contents {
    now: Vec<u8>,
    safe: Option<Vec<u8>>,
}

/// How unkind the disk is about what was not made safe when the power goes.
#[derive(Clone, Copy, Debug)]
struct Unkind {
    /// Whether renames that were not made safe happened anyway.
    names_kept: bool,
    /// What is left of file contents that were not made safe: 0 nothing, 1 half, 2 all.
    contents_kept: u8,
}

/// A disk that counts its steps and loses power after a given one.
#[derive(Clone, Debug, Default)]
struct ModelDisk {
    files: Vec<Contents>,
    /// Names as a running program sees them, and as they are safe on the disk.
    names: BTreeMap<PathBuf, usize>,
    safe_names: BTreeMap<PathBuf, usize>,
    steps: usize,
    power_goes_after: Option<usize>,
}

impl ModelDisk {
    fn step(&mut self) -> io::Result<()> {
        if self
            .power_goes_after
            .is_some_and(|after| self.steps >= after)
        {
            return Err(io::Error::other("the power went"));
        }
        self.steps += 1;
        Ok(())
    }

    /// The disk as it is found when the power comes back.
    fn after_power_loss(&self, unkind: Unkind) -> Self {
        let names = if unkind.names_kept {
            &self.names
        } else {
            &self.safe_names
        };
        let mut found = Self::default();
        for (name, index) in names {
            let contents = &self.files[*index];
            let bytes = match (&contents.safe, unkind.contents_kept) {
                (Some(safe), _) => safe.clone(),
                (None, 0) => Vec::new(),
                (None, 1) => contents.now[..contents.now.len() / 2].to_vec(),
                (None, _) => contents.now.clone(),
            };
            found.files.push(Contents {
                now: bytes.clone(),
                safe: Some(bytes),
            });
            found.names.insert(name.clone(), found.files.len() - 1);
        }
        found.safe_names = found.names.clone();
        found
    }

    fn file_names(&self) -> Vec<String> {
        self.names
            .keys()
            .map(|path| {
                path.file_name()
                    .expect("name")
                    .to_string_lossy()
                    .into_owned()
            })
            .collect()
    }
}

impl Disk for ModelDisk {
    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        match self.names.get(path) {
            Some(index) => Ok(self.files[*index].now.clone()),
            None => Err(io::ErrorKind::NotFound.into()),
        }
    }

    fn write(&mut self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        self.step()?;
        self.files.push(Contents {
            now: bytes.to_vec(),
            safe: None,
        });
        self.names.insert(path.to_owned(), self.files.len() - 1);
        Ok(())
    }

    fn sync(&mut self, path: &Path) -> io::Result<()> {
        self.step()?;
        let index = *self.names.get(path).ok_or(io::ErrorKind::NotFound)?;
        self.files[index].safe = Some(self.files[index].now.clone());
        Ok(())
    }

    fn rename(&mut self, from: &Path, to: &Path) -> io::Result<()> {
        if !self.names.contains_key(from) {
            return Err(io::ErrorKind::NotFound.into());
        }
        self.step()?;
        let index = self.names.remove(from).expect("checked");
        self.names.insert(to.to_owned(), index);
        Ok(())
    }

    fn sync_dir(&mut self, _dir: &Path) -> io::Result<()> {
        self.step()?;
        self.safe_names = self.names.clone();
        Ok(())
    }

    fn remove(&mut self, path: &Path) -> io::Result<()> {
        if self.names.contains_key(path) {
            self.step()?;
            self.names.remove(path);
        }
        Ok(())
    }
}

fn dir() -> &'static Path {
    Path::new("/state")
}

/// A set of records that says which generation it is.
fn generation(number: u64) -> Vec<Record> {
    (0..20)
        .map(|index| Record {
            class: if index % 5 == 0 {
                RecordClass::Node
            } else {
                RecordClass::Resource
            },
            key: format!("record.{index:02}"),
            version: Version {
                stamp: HlcTimestamp::new(1_800_000_000_000 + number, index, 7),
                deleted: index % 7 == 3,
                value: if index % 7 == 3 {
                    Vec::new()
                } else {
                    format!("generation {number}, record {index}").into_bytes()
                },
            },
        })
        .collect()
}

#[test]
fn power_can_go_at_any_step_of_a_save_and_a_whole_snapshot_is_found() {
    // A disk that already holds generation 1 and 2, so there is a snapshot and one before it.
    let mut persist = Persist::on(dir(), ModelDisk::default());
    persist.save(generation(1).into_iter()).expect("save");
    persist.save(generation(2).into_iter()).expect("save");
    let settled = persist.into_disk();
    let steps_before = settled.steps;
    // How many steps a save takes, found by doing one.
    let mut whole = Persist::on(dir(), settled.clone());
    whole.save(generation(3).into_iter()).expect("save");
    let steps_in_a_save = whole.disk().steps - steps_before;
    assert!(
        steps_in_a_save >= 5,
        "write, sync, two renames and a directory sync"
    );

    let mut outcomes = BTreeMap::new();
    for step in 0..=steps_in_a_save {
        for names_kept in [false, true] {
            for contents_kept in 0..3 {
                let unkind = Unkind {
                    names_kept,
                    contents_kept,
                };
                let mut disk = settled.clone();
                disk.power_goes_after = Some(steps_before + step);
                let mut saving = Persist::on(dir(), disk);
                let saved = saving.save(generation(3).into_iter());
                assert_eq!(saved.is_ok(), step == steps_in_a_save, "step {step}");

                // The power comes back. Whatever the disk kept, the node starts.
                let found = saving.into_disk().after_power_loss(unkind);
                let mut starting = Persist::on(dir(), found);
                let loaded = starting.load();
                let which = [1, 2, 3]
                    .into_iter()
                    .find(|number| loaded.records == generation(*number));
                assert!(
                    which.is_some(),
                    "step {step}, {unkind:?}: neither one snapshot nor another: {:?}",
                    loaded.problems
                );
                // A save that reported success is what is found, however unkind the disk.
                if saved.is_ok() {
                    assert_eq!(which, Some(3), "step {step}, {unkind:?}");
                }
                // And never the snapshot from two saves ago unless the newest was lost whole.
                if which == Some(1) {
                    assert_eq!(loaded.source, Source::Previous, "step {step}, {unkind:?}");
                }
                *outcomes.entry(which.expect("checked")).or_insert(0) += 1;

                // It carries on: the next save and the next start are as any other.
                starting
                    .save(generation(4).into_iter())
                    .expect("the next save");
                let after =
                    Persist::on(dir(), starting.into_disk().after_power_loss(unkind)).load();
                assert_eq!(
                    (after.records, after.source),
                    (generation(4), Source::Current),
                    "step {step}, {unkind:?}"
                );
            }
        }
    }
    // Both outcomes really happened: the test went through the old and the new.
    assert!(
        outcomes.contains_key(&2) && outcomes.contains_key(&3),
        "{outcomes:?}"
    );
}

#[test]
fn a_file_that_is_not_a_snapshot_never_stops_start_up() {
    let name = format!("records.v{FORMAT}.snap");
    let mut persist = Persist::on(dir(), ModelDisk::default());
    // Nothing at all: a new board.
    assert_eq!(
        persist.load(),
        Loaded {
            records: Vec::new(),
            source: Source::Nothing,
            problems: Vec::new()
        }
    );
    persist.save(generation(1).into_iter()).expect("save");
    persist.save(generation(2).into_iter()).expect("save");
    let good = persist
        .disk()
        .read(&dir().join(&name))
        .expect("the snapshot");

    // Every way the newest snapshot can be wrong: the one before it is used, the bad one is
    // moved aside, and the reason is said.
    let mut flipped = good.clone();
    flipped[good.len() / 2] ^= 0x10;
    let mut other_format = good.clone();
    other_format[8] = 9;
    let cases: [(&str, Vec<u8>, &str); 6] = [
        ("empty", Vec::new(), "cut short"),
        ("cut short", good[..good.len() - 9].to_vec(), "checksum"),
        ("one bit changed", flipped, "checksum"),
        (
            "not a snapshot",
            b"this is a text file someone left here, long enough".to_vec(),
            "not a snapshot",
        ),
        ("zeros", vec![0; good.len()], "not a snapshot"),
        ("another format inside", other_format, "checksum"),
    ];
    for (what, bytes, says) in cases {
        let mut disk = persist.disk().clone();
        disk.write(&dir().join(&name), &bytes).expect("damage");
        let mut starting = Persist::on(dir(), disk);
        let loaded = starting.load();
        assert_eq!(
            (loaded.records, loaded.source),
            (generation(1), Source::Previous),
            "{what}"
        );
        assert_eq!(loaded.problems.len(), 1, "{what}");
        assert!(
            loaded.problems[0].contains(says) && loaded.problems[0].contains("moved aside"),
            "{what}: {:?}",
            loaded.problems
        );
        let names = starting.disk().file_names();
        assert!(
            names.iter().any(|kept| kept.contains("unreadable")),
            "{what}: {names:?}"
        );
        assert!(
            !names.contains(&name),
            "{what}: the bad file is out of the way"
        );
        // The next start does not trip on it again, and the next save is as any other.
        let again = starting.load();
        assert_eq!(
            (again.source, again.problems.len()),
            (Source::Previous, 0),
            "{what}"
        );
        starting.save(generation(3).into_iter()).expect("save");
        assert_eq!(starting.load().records, generation(3), "{what}");
    }

    // Both gone bad: the node starts with nothing, which its peers put right.
    let mut disk = persist.disk().clone();
    disk.write(&dir().join(&name), b"junk").expect("damage");
    disk.write(&dir().join(format!("{name}.prev")), b"more junk")
        .expect("damage");
    let mut starting = Persist::on(dir(), disk);
    let loaded = starting.load();
    assert_eq!(
        (loaded.records.len(), loaded.source, loaded.problems.len()),
        (0, Source::Nothing, 2)
    );
    starting.save(generation(5).into_iter()).expect("save");
    assert_eq!(starting.load().records, generation(5));
}

#[test]
fn a_store_comes_back_from_the_real_disk_as_it_was() {
    let dir = std::env::temp_dir().join(format!("orion-persist-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let name: orion_types::NodeName = "node-a".parse().expect("name");
    let mut store = Store::new(name.clone(), StoreConfig::default());
    for index in 0..300_u32 {
        store.put(
            RecordClass::Resource,
            &format!("item.{index:03}"),
            &index.to_le_bytes(),
            1_800_000_000_000,
        );
    }
    store.delete(RecordClass::Resource, "item.007", 1_800_000_000_001);
    store.put(
        RecordClass::Node,
        "node-b",
        &[0xFF; 3000],
        1_800_000_000_002,
    );
    let mut persist = Persist::open(&dir).expect("open");
    persist.save(store.all()).expect("save");
    persist.save(store.all()).expect("save again");

    // Another process, later.
    let mut later = Persist::open(&dir).expect("open");
    let loaded = later.load();
    assert_eq!((loaded.source, loaded.problems.len()), (Source::Current, 0));
    let back = Store::restored(name, StoreConfig::default(), loaded.records);
    assert_eq!(
        back.all().collect::<Vec<_>>(),
        store.all().collect::<Vec<_>>()
    );
    assert_eq!(back.fingerprint(), store.fingerprint());
    assert!(
        back.get(RecordClass::Resource, "item.007").is_none(),
        "the delete came back as a delete"
    );
    // Only the snapshot and the one before it are left: nothing half written.
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .expect("dir")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    assert_eq!(
        names,
        [
            format!("records.v{FORMAT}.snap"),
            format!("records.v{FORMAT}.snap.prev")
        ]
    );

    // The real file, damaged for real: the one before it is used.
    let path = later.path();
    let mut bytes = std::fs::read(&path).expect("read");
    let middle = bytes.len() / 2;
    bytes[middle] ^= 1;
    std::fs::write(&path, &bytes).expect("damage");
    let loaded = Persist::open(&dir).expect("open").load();
    assert_eq!(
        (loaded.source, loaded.records.len()),
        (Source::Previous, store.len())
    );
    let _ = std::fs::remove_dir_all(dir);
}
