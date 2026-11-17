//! Orion's answer to "may the board's security switch be turned?", asked before it is.
//!
//! The device library asks every part of the board before it turns the switch, and turns it
//! only when none says no. This is Orion's part. It changes nothing; it says what would be
//! wrong, in the words of `system.security-precondition/1`: one `missing: <reason>` or
//! `busy: <reason>` line for each thing against it, and none when nothing is.
//!
//! - **To on**: something other than root must be able to get in once it is on, or the board
//!   is shut to everyone off it. That is an enrolled operator, a key in the recovery keys
//!   file, or an enrollment key that lets a newcomer enrol itself. In recovery the trust store
//!   is not opened, so there only the recovery keys file counts: that is who recovery lets in.
//! - **To off**: nothing on Orion's side is against it. Whether the turn-off is authorized is
//!   the writer's to check, not this answer's.
//! - Orion is never `busy`: it installs and tries nothing that a turn would cut short.

use std::path::PathBuf;

use orion_keys::SharedKeys;
use orion_msg_peers::{
    OperatorState, PrecheckLine, SecurityPrecheck, SecurityPrechecked, SecurityTo,
};
use orion_server::{
    Admits, Checked, Cx, Error, Incoming, Policy, Service, SessionId, Sessions, What,
};
use orion_wire::{FrameView, List, kind};

/// Answers [`SecurityPrecheck`].
pub(crate) struct Precheck {
    /// The trust store, when it is open: `None` in recovery.
    keys: Option<SharedKeys>,
    /// The recovery keys file, when this build has recovery.
    recovery_keys: Option<PathBuf>,
}

impl std::fmt::Debug for Precheck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Precheck")
    }
}

impl Precheck {
    /// The answer from `keys`, the open trust store if there is one, and the recovery keys
    /// file at `recovery_keys`.
    pub(crate) const fn new(keys: Option<SharedKeys>, recovery_keys: Option<PathBuf>) -> Self {
        Self {
            keys,
            recovery_keys,
        }
    }

    /// What is against turning the switch `to`.
    fn against(&self, to: SecurityTo) -> Vec<String> {
        if to != SecurityTo::On {
            return Vec::new();
        }
        let (enrolled, enrollment) = self.keys.as_ref().map_or((false, false), |keys| {
            let keys = keys.lock();
            (
                keys.operators()
                    .any(|operator| operator.state == OperatorState::Enrolled),
                keys.enrollment_key().is_some(),
            )
        });
        if enrolled || enrollment || self.recovery_key().is_some() {
            return Vec::new();
        }
        let file = self.recovery_keys.as_ref().map_or_else(
            || "the recovery keys file".to_owned(),
            |file| file.display().to_string(),
        );
        vec![format!(
            "missing: orion: nobody but root could get in once it is on: enroll an operator, \
             add a key to {file}, or set enrollment.key"
        )]
    }

    /// A key the recovery keys file lets in, if it names one.
    #[cfg(feature = "recovery")]
    fn recovery_key(&self) -> Option<[u8; 32]> {
        let text = std::fs::read_to_string(self.recovery_keys.as_ref()?).ok()?;
        orion_recovery::keys_in(&text).first().map(|(key, _)| *key)
    }

    #[cfg(not(feature = "recovery"))]
    fn recovery_key(&self) -> Option<[u8; 32]> {
        None
    }
}

impl Service for Precheck {
    fn kinds(&self) -> &'static [u8] {
        &[kind::SECURITY_PRECHECK]
    }

    fn asks<'a>(&self, _frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        // Programs on the board (the device library's bridge among them) and operators
        // granted read. It tells which ways in exist, not who holds them.
        Ok((
            What::Read("security"),
            Policy::new(Admits::LOCAL.and(Admits::OPERATOR)),
        ))
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let precheck: SecurityPrecheck = request.into_inner().body()?;
        if !matches!(precheck.to, SecurityTo::On | SecurityTo::Off) {
            return Err(Error::Malformed);
        }
        let against = self.against(precheck.to);
        let lines: Vec<PrecheckLine<'_>> =
            against.iter().map(|line| PrecheckLine { line }).collect();
        cx.reply(&SecurityPrechecked {
            request_seq: cx.request_seq(),
            lines: List::new(&lines),
        })
    }

    fn quiet(&mut self, _session: SessionId, _quiet_ms: u64, _sessions: &mut Sessions) {}
}

#[cfg(test)]
mod tests {
    use orion_config::Security;
    use orion_keys::{Keys, KeysConfig, SharedKeys};

    use super::*;

    #[test]
    fn only_an_enrolled_operator_is_a_way_in() {
        let keys = Keys::open(KeysConfig::in_memory("lab"), Security::On).expect("keys");
        let keys = SharedKeys::new(keys);
        let precheck = Precheck::new(Some(keys.clone()), None);
        let node = "board".parse().expect("name");
        // Asking without a proof leaves the operator pending: not yet a way in.
        let _ = keys.lock().operator_asks(&node, [7; 32], "pending", &[]);
        assert_eq!(precheck.against(SecurityTo::On).len(), 1);
        keys.lock()
            .approve_operator([7; 32], "now")
            .expect("approve");
        assert!(precheck.against(SecurityTo::On).is_empty());
        // Revoked, it is no way in again.
        keys.lock().revoke([7; 32]).expect("revoke");
        assert_eq!(precheck.against(SecurityTo::On).len(), 1);
        assert!(precheck.against(SecurityTo::Off).is_empty());
    }
}
