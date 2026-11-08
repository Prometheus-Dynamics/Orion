//! Which build's state layout a state directory holds, and where this build keeps its own.
//!
//! The cluster state files in the state root belong to the build that wrote them. A build that
//! cannot read them (a rollback, another slot, an older node on newer state) never moves or
//! rewrites them: it keeps its own state under `layouts/<its stamp>/` and runs normally, on
//! defaults the first time. When the writer boots again, its files are where it left them.
//!
//! A section that does not decode although the stamp names this build is damage, which is
//! quarantined and starts safe mode. The stamp covers the sources of the persisted types (see
//! `build.rs`), so an unrelated edit to those sources changes it too; that only means the full
//! decode below runs once.

use super::{CLUSTER_STATE_FILES, MAINTENANCE_STATE_FILE, NodeStorage};
use crate::NodeError;
use std::path::{Path, PathBuf};

const STATE_LAYOUT_FILE: &str = "state-layout";
const LAYOUTS_DIR: &str = "layouts";
const THIS_LAYOUT: &str = env!("ORION_STATE_LAYOUT");

impl NodeStorage {
    /// Whether the stamp in the directory this build uses names this build's layout. A directory
    /// without one was written before stamps existed, so by another build.
    pub fn written_by_this_layout(&self) -> bool {
        std::fs::read_to_string(self.state_root.join(STATE_LAYOUT_FILE))
            .is_ok_and(|stamp| stamp.trim() == THIS_LAYOUT)
    }

    /// Records this build's layout, once the state has been read. Only writes when the stamp
    /// changes.
    pub fn stamp_state_layout(&self) -> Result<(), NodeError> {
        if self.written_by_this_layout() {
            return Ok(());
        }
        self.ensure_layout()?;
        self.atomic_write(
            &self.state_root.join(STATE_LAYOUT_FILE),
            THIS_LAYOUT.as_bytes(),
        )
    }

    /// Picks where this build keeps its cluster state: the state root, or its own directory when
    /// the root holds state it cannot read. Returns the directory when it is not the root.
    pub fn settle_state_root(&mut self) -> Result<Option<PathBuf>, NodeError> {
        if !self.root_holds_unreadable_state()? {
            return Ok(None);
        }
        self.state_root = self.own_layout_dir()?;
        Ok(Some(self.state_root.clone()))
    }

    fn own_layout_dir(&self) -> Result<PathBuf, NodeError> {
        let dir = self.root.join(LAYOUTS_DIR).join(THIS_LAYOUT);
        std::fs::create_dir_all(&dir).map_err(|err| NodeError::Storage(err.to_string()))?;
        Ok(dir)
    }

    pub(crate) fn trust_root(&self) -> &Path {
        &self.trust_root
    }

    /// The same rule for the peer trust store: one this build cannot read is left untouched and
    /// the node runs without those peers, keeping its own store beside it. A store that fails
    /// must never stop the node from starting. Returns whether the root's store was unreadable.
    pub(crate) fn settle_trust_store(
        &mut self,
        readable: impl Fn(&[u8]) -> bool,
    ) -> Result<bool, NodeError> {
        let readable_at = |path: &Path| std::fs::read(path).is_ok_and(|bytes| readable(&bytes));
        let path = self.trust_store_path();
        if !path.exists() || readable_at(&path) {
            return Ok(false);
        }
        self.trust_root = self.own_layout_dir()?;
        let own = self.trust_store_path();
        if own.exists() && !readable_at(&own) {
            // This build's own copy is damaged; it is kept, and the store starts empty.
            let aside = own.with_extension(format!("damaged-{}", std::process::id()));
            std::fs::rename(&own, aside).map_err(|err| NodeError::Storage(err.to_string()))?;
        }
        Ok(true)
    }

    fn root_holds_unreadable_state(&self) -> Result<bool, NodeError> {
        let present = CLUSTER_STATE_FILES
            .iter()
            .chain([&MAINTENANCE_STATE_FILE])
            .any(|name| self.root.join(name).exists());
        if !present {
            return Ok(false);
        }
        if self.unsupported_snapshot_format()?.is_some() {
            return Ok(true);
        }
        if self.written_by_this_layout() {
            return Ok(false);
        }
        Ok(self.load_snapshot().is_err()
            || self.load_mutation_history().is_err()
            || self.load_mutation_history_baseline().is_err()
            || self.load_maintenance_state().is_err())
    }
}
