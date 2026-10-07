//! Extra local IPC callers admitted on top of `ORION_NODE_LOCAL_AUTH`
//! (`ORION_NODE_LOCAL_AUTH_ALLOW`, see `docs/node-env.md`).

use crate::NodeError;
use orion_transport_ipc::UnixPeerIdentity;
use std::collections::BTreeSet;
use std::ffi::CString;

/// Environment variable holding the allow-list.
pub const LOCAL_AUTH_ALLOW_ENV: &str = "ORION_NODE_LOCAL_AUTH_ALLOW";

/// Local callers admitted in addition to the [`LocalAuthenticationMode`](super::LocalAuthenticationMode)
/// (which keeps its meaning): users by uid and groups by gid. A group admits a caller whose
/// primary group or one of whose supplementary groups it is.
///
/// Parsed from comma-separated entries: `root` (uid 0), `uid:<n>`, `gid:<n>`, `user:<name>` and
/// `group:<name>` (names are resolved once, at startup, through the system user database; an
/// unknown name fails startup).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LocalAccessAllowList {
    uids: BTreeSet<u32>,
    gids: BTreeSet<u32>,
}

impl LocalAccessAllowList {
    /// Admits nobody beyond the authentication mode.
    pub fn new() -> Self {
        Self::default()
    }

    /// Also admits the user `uid`.
    pub fn allow_uid(mut self, uid: u32) -> Self {
        self.uids.insert(uid);
        self
    }

    /// Also admits members of the group `gid` (primary or supplementary).
    pub fn allow_gid(mut self, gid: u32) -> Self {
        self.gids.insert(gid);
        self
    }

    pub fn uids(&self) -> &BTreeSet<u32> {
        &self.uids
    }

    pub fn gids(&self) -> &BTreeSet<u32> {
        &self.gids
    }

    pub fn is_empty(&self) -> bool {
        self.uids.is_empty() && self.gids.is_empty()
    }

    /// Whether the allow-list admits `identity`.
    pub fn admits(&self, identity: &UnixPeerIdentity) -> bool {
        self.uids.contains(&identity.uid) || self.gids.iter().any(|gid| identity.is_member_of(*gid))
    }

    /// Parses an allow-list (see the type docs). Blank entries are ignored.
    pub fn parse(spec: &str) -> Result<Self, NodeError> {
        let mut list = Self::new();
        for entry in spec.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            let invalid = |reason: &str| {
                NodeError::Config(format!(
                    "invalid {LOCAL_AUTH_ALLOW_ENV} entry `{entry}`: {reason}; expected `root`, \
                     `uid:<n>`, `gid:<n>`, `user:<name>` or `group:<name>`"
                ))
            };
            if entry.eq_ignore_ascii_case("root") {
                list.uids.insert(0);
                continue;
            }
            let Some((kind, value)) = entry.split_once(':') else {
                return Err(invalid("missing kind"));
            };
            let value = value.trim();
            match kind.trim().to_ascii_lowercase().as_str() {
                "uid" => {
                    list.uids
                        .insert(value.parse().map_err(|_| invalid("not a number"))?);
                }
                "gid" => {
                    list.gids
                        .insert(value.parse().map_err(|_| invalid("not a number"))?);
                }
                "user" => {
                    let uid = lookup_user(value).ok_or_else(|| invalid("unknown user"))?;
                    list.uids.insert(uid);
                }
                "group" => {
                    let gid = lookup_group(value).ok_or_else(|| invalid("unknown group"))?;
                    list.gids.insert(gid);
                }
                _ => return Err(invalid("unknown kind")),
            }
        }
        Ok(list)
    }

    /// Reads `ORION_NODE_LOCAL_AUTH_ALLOW` (empty when unset).
    pub fn try_from_env() -> Result<Self, NodeError> {
        match std::env::var(LOCAL_AUTH_ALLOW_ENV) {
            Ok(value) => Self::parse(&value),
            Err(std::env::VarError::NotPresent) => Ok(Self::new()),
            Err(std::env::VarError::NotUnicode(_)) => Err(NodeError::Config(format!(
                "{LOCAL_AUTH_ALLOW_ENV} must be valid unicode"
            ))),
        }
    }
}

/// Grows the scratch buffer of a `get*nam_r` call until the entry fits (bounded).
fn with_lookup_buffer(mut call: impl FnMut(&mut [libc::c_char]) -> libc::c_int) -> bool {
    let mut buffer = vec![0 as libc::c_char; 1024];
    loop {
        match call(&mut buffer) {
            0 => return true,
            libc::ERANGE if buffer.len() < (1 << 20) => {
                let len = buffer.len() * 2;
                buffer.resize(len, 0);
            }
            _ => return false,
        }
    }
}

fn lookup_user(name: &str) -> Option<u32> {
    let name = CString::new(name).ok()?;
    // SAFETY: an all-zero `passwd` is a valid value for getpwnam_r to overwrite.
    let mut entry: libc::passwd = unsafe { std::mem::zeroed() };
    let mut found: *mut libc::passwd = std::ptr::null_mut();
    let ok = with_lookup_buffer(|buffer| {
        // SAFETY: every pointer refers to live memory owned by this frame; `buffer.len()` is
        // the buffer's size.
        unsafe {
            libc::getpwnam_r(
                name.as_ptr(),
                &mut entry,
                buffer.as_mut_ptr(),
                buffer.len(),
                &mut found,
            )
        }
    });
    (ok && !found.is_null()).then_some(entry.pw_uid)
}

fn lookup_group(name: &str) -> Option<u32> {
    let name = CString::new(name).ok()?;
    // SAFETY: an all-zero `group` is a valid value for getgrnam_r to overwrite.
    let mut entry: libc::group = unsafe { std::mem::zeroed() };
    let mut found: *mut libc::group = std::ptr::null_mut();
    let ok = with_lookup_buffer(|buffer| {
        // SAFETY: as in `lookup_user`.
        unsafe {
            libc::getgrnam_r(
                name.as_ptr(),
                &mut entry,
                buffer.as_mut_ptr(),
                buffer.len(),
                &mut found,
            )
        }
    });
    (ok && !found.is_null()).then_some(entry.gr_gid)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(uid: u32, gid: u32, groups: &[u32]) -> UnixPeerIdentity {
        UnixPeerIdentity {
            pid: None,
            uid,
            gid,
            groups: groups.to_vec(),
        }
    }

    #[test]
    fn parses_numeric_and_root_entries() {
        let list = LocalAccessAllowList::parse(" root, uid:1000 ,gid:27,,GID:28").expect("parse");
        assert_eq!(
            list.uids().iter().copied().collect::<Vec<_>>(),
            vec![0, 1000]
        );
        assert_eq!(
            list.gids().iter().copied().collect::<Vec<_>>(),
            vec![27, 28]
        );
        assert!(LocalAccessAllowList::parse("").expect("empty").is_empty());
    }

    #[test]
    fn resolves_names_from_the_user_database() {
        let list = LocalAccessAllowList::parse("user:root,group:root").expect("root exists");
        assert!(list.uids().contains(&0));
        assert!(list.gids().contains(&0));
    }

    #[test]
    fn rejects_malformed_and_unknown_entries() {
        for bad in [
            "wheel",
            "uid:abc",
            "gid:-1",
            "host:x",
            "user:orion-no-such-user-xyz",
            "group:orion-no-such-group-xyz",
        ] {
            let err = LocalAccessAllowList::parse(bad).expect_err(bad);
            assert!(
                matches!(&err, NodeError::Config(message) if message.contains(LOCAL_AUTH_ALLOW_ENV)),
                "{bad}: {err:?}"
            );
        }
    }

    #[test]
    fn admits_listed_users_and_members_of_listed_groups() {
        let list = LocalAccessAllowList::new().allow_uid(0).allow_gid(990);
        assert!(list.admits(&identity(0, 0, &[])));
        assert!(list.admits(&identity(1000, 990, &[])));
        assert!(list.admits(&identity(1000, 1000, &[4, 990])));
        assert!(!list.admits(&identity(1000, 1000, &[4, 27])));
        assert!(!LocalAccessAllowList::new().admits(&identity(0, 0, &[])));
    }
}
