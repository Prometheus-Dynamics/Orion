//! What a node must know before it is asked for something added since the level began.
//!
//! An addition an older node would ignore without changing what it does needs no level. One whose
//! meaning it would change by ignoring it (a subscriber that asked for the newest value and
//! silently gets none) or that it would not answer at all (a shell) has a [`Feature`], and the
//! client asks for it only of a node whose `Welcome` names that feature's level or a higher one.
//! Otherwise the call fails with [`ClientError::Level`](crate::ClientError::Level) and sends
//! nothing.

use std::fmt;

use crate::{Client, ClientError, Session, Welcomed};

/// Something a client may ask only of a node that knows it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Feature {
    /// A topic declared so that a new subscriber starts from its newest values
    /// (`OnSubscribe::Latest`).
    SubscribeFromLatest,
    /// A shell opened over the session.
    Shell,
    /// A session on a serial or USB link signed from the first frame.
    SignedLink,
    /// A run that carries on without its caller, under the node's deadline rules.
    DetachedRun,
    /// A run carried for a caller on another node, whose end is told on its own.
    RunOnBehalf,
}

impl Feature {
    /// Every feature, for tables and tests.
    pub const ALL: [Self; 5] = [
        Self::SubscribeFromLatest,
        Self::Shell,
        Self::SignedLink,
        Self::DetachedRun,
        Self::RunOnBehalf,
    ];

    /// The lowest level that has it.
    #[must_use]
    pub const fn level(self) -> u16 {
        match self {
            Self::SubscribeFromLatest
            | Self::Shell
            | Self::SignedLink
            | Self::DetachedRun
            | Self::RunOnBehalf => 1,
        }
    }

    /// What it is called in errors and logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SubscribeFromLatest => "a topic whose subscribers start from its newest values",
            Self::Shell => "a shell over the session",
            Self::SignedLink => "a signed session on a link",
            Self::DetachedRun => "a detached run",
            Self::RunOnBehalf => "a run carried for another node's caller",
        }
    }
}

impl fmt::Display for Feature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Welcomed {
    /// Whether the node is at `feature`'s level, so it may be asked for it.
    ///
    /// # Errors
    ///
    /// [`ClientError::Level`], naming the feature and both levels, if it is not.
    pub const fn needs(&self, feature: Feature) -> Result<(), ClientError> {
        if self.level < feature.level() {
            return Err(ClientError::Level {
                feature,
                needs: feature.level(),
                node: self.level,
            });
        }
        Ok(())
    }
}

impl Session {
    /// As [`Welcomed::needs`], for this session's node.
    ///
    /// # Errors
    ///
    /// [`ClientError::Level`] if the node is below `feature`'s level.
    pub const fn needs(&self, feature: Feature) -> Result<(), ClientError> {
        self.welcomed().needs(feature)
    }
}

impl Client {
    /// As [`Welcomed::needs`], for this client's node. Not from inside a handler.
    ///
    /// # Errors
    ///
    /// [`ClientError::Level`] if the node is below `feature`'s level.
    pub fn needs(&self, feature: Feature) -> Result<(), ClientError> {
        self.session(|session| session.needs(feature))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(level: u16) -> Welcomed {
        Welcomed {
            node: "board".parse().expect("name"),
            session: 1,
            heartbeat: std::time::Duration::from_secs(1),
            idle: std::time::Duration::from_secs(3),
            max_frame: 4096,
            revision: orion_wire::INTERFACE_REVISION,
            level,
        }
    }

    #[test]
    fn a_feature_is_asked_of_a_node_at_its_level_or_above_and_no_other() {
        for feature in Feature::ALL {
            let needs = feature.level();
            assert!(at(needs).needs(feature).is_ok());
            assert!(at(needs + 1).needs(feature).is_ok());
            let refused = at(needs - 1).needs(feature).expect_err("below");
            assert!(
                matches!(refused, ClientError::Level { feature: f, needs: n, node }
                    if f == feature && n == needs && node == needs - 1),
                "{refused:?}"
            );
            // The words name the feature and both levels.
            let said = refused.to_string();
            assert!(said.contains(feature.as_str()) && said.contains(&format!("level {needs}")));
        }
    }

    #[test]
    fn no_feature_is_above_the_level_this_build_speaks() {
        for feature in Feature::ALL {
            assert!(
                (1..=orion_wire::INTERFACE_LEVEL).contains(&feature.level()),
                "{feature}"
            );
        }
    }
}
