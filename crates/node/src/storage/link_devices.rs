//! Which providers link devices own (`link-devices.rkyv` in the state directory). A restarted node
//! reads it to mark those providers unavailable until their device reconnects, and to keep the
//! providers reserved for their device. It is a separate file, so the cluster snapshot format does
//! not change. Written by `app/link_bridge/persist.rs`; see `docs/link-protocol.md` ("Restart").

use super::{NodeStorage, decode_from_slice, encode_to_vec};
use crate::NodeError;
use orion::control_plane::{ProviderRecord, ResourceRecord};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

const LINK_DEVICES_FILE: &str = "link-devices.rkyv";
const LINK_DEVICES_FORMAT_VERSION: u32 = 1;

/// One provider a link device published, with the last provider and resource records the node
/// accepted for it.
#[derive(Clone, Debug, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(crate) struct LinkDeviceRecord {
    pub(crate) link: String,
    pub(crate) device: String,
    pub(crate) provider: ProviderRecord,
    pub(crate) resources: Vec<ResourceRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
struct LinkDevicesFile {
    format_version: u32,
    devices: Vec<LinkDeviceRecord>,
}

impl NodeStorage {
    /// The persisted link-owned providers. Empty when the file does not exist.
    pub(crate) fn load_link_devices(&self) -> Result<Vec<LinkDeviceRecord>, NodeError> {
        let path = self.root().join(LINK_DEVICES_FILE);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let bytes = std::fs::read(&path).map_err(|err| NodeError::Storage(err.to_string()))?;
        let file: LinkDevicesFile = decode_from_slice(&bytes).map_err(|err| {
            NodeError::Storage(format!(
                "link devices file {} is not readable: {err}",
                path.display()
            ))
        })?;
        if file.format_version != LINK_DEVICES_FORMAT_VERSION {
            return Err(NodeError::Storage(format!(
                "unsupported link devices format version {}",
                file.format_version
            )));
        }
        Ok(file.devices)
    }

    /// Replaces the persisted set atomically (write, fsync, rename).
    pub(crate) fn save_link_devices(&self, devices: &[LinkDeviceRecord]) -> Result<(), NodeError> {
        self.ensure_layout()?;
        let file = LinkDevicesFile {
            format_version: LINK_DEVICES_FORMAT_VERSION,
            devices: devices.to_vec(),
        };
        let bytes = encode_to_vec(&file)?;
        self.atomic_write(&self.root().join(LINK_DEVICES_FILE), &bytes)
    }
}
