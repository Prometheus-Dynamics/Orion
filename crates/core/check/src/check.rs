use core::fmt;

use orion_types::{Error, KeyId};
use serde::{Deserialize, Serialize};

use crate::{Policy, Principal};

/// The one switch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Security {
    /// The check admits everything without looking at the policy.
    Off,
    /// The check runs.
    On,
}

/// What a request wants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum What<'a> {
    /// Read the named thing.
    Read(&'a str),
    /// Change the named thing.
    Write(&'a str),
    /// Run the named action.
    Run(&'a str),
    /// Open the named topic or connection.
    Open(&'a str),
    /// Log in.
    Login,
    /// Open a shell as the named account.
    Shell(&'a str),
}

/// A request the check can decide on.
pub trait Request {
    /// What it wants.
    fn what(&self) -> What<'_>;
}

/// What an individual operator has been granted, beyond being an operator at all.
pub trait Grants: Sync {
    /// Whether the operator with `key` may do `what`.
    fn operator_may(&self, key: &KeyId, what: What<'_>) -> bool;
}

struct EveryOperator;

impl Grants for EveryOperator {
    fn operator_may(&self, _key: &KeyId, _what: What<'_>) -> bool {
        true
    }
}

static EVERY_OPERATOR: EveryOperator = EveryOperator;

/// Why the check said no.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refused {
    /// The policy doesn't admit this class of caller.
    NotAdmitted,
    /// This operator hasn't been granted this.
    NotGranted,
}

impl Refused {
    /// The stable error for this refusal.
    #[must_use]
    pub const fn error(self) -> Error {
        Error::Denied
    }
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NotAdmitted => "the policy does not admit this caller",
            Self::NotGranted => "this operator has not been granted this",
        })
    }
}

impl core::error::Error for Refused {}

/// A request that passed the check. Only [`Check::decide`] can make one.
#[derive(Debug)]
pub struct Checked<T> {
    value: T,
    #[cfg(feature = "enforce")]
    who: Principal,
}

#[cfg(not(feature = "enforce"))]
static NOBODY_KNOWN: Principal = Principal::Anonymous;

impl<T> Checked<T> {
    /// Who asked. Built without feature `enforce`, always [`Principal::Anonymous`].
    #[must_use]
    pub fn who(&self) -> &Principal {
        #[cfg(feature = "enforce")]
        return &self.who;
        #[cfg(not(feature = "enforce"))]
        return &NOBODY_KNOWN;
    }

    /// The request.
    #[must_use]
    pub const fn get(&self) -> &T {
        &self.value
    }

    /// The request, by value.
    #[must_use]
    pub fn into_inner(self) -> T {
        self.value
    }
}

/// The check. Copy it freely: it is a switch and a reference.
#[derive(Clone, Copy)]
pub struct Check<'g> {
    #[cfg(feature = "enforce")]
    security: Security,
    #[cfg(feature = "enforce")]
    grants: &'g dyn Grants,
    #[cfg(not(feature = "enforce"))]
    grants: core::marker::PhantomData<&'g ()>,
}

impl fmt::Debug for Check<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Check")
    }
}

impl Check<'static> {
    /// A check where every enrolled operator has every grant.
    #[must_use]
    pub fn new(security: Security) -> Self {
        Self::with_grants(security, &EVERY_OPERATOR)
    }
}

impl<'g> Check<'g> {
    /// A check consulting `grants` for individual operators.
    #[cfg(feature = "enforce")]
    #[must_use]
    pub fn with_grants(security: Security, grants: &'g dyn Grants) -> Self {
        Self { security, grants }
    }

    /// Built without feature `enforce` there is nothing to keep.
    #[cfg(not(feature = "enforce"))]
    #[must_use]
    pub fn with_grants(_security: Security, _grants: &'g dyn Grants) -> Self {
        Self {
            grants: core::marker::PhantomData,
        }
    }

    /// Decides on `request` from `who` under `policy`.
    ///
    /// # Errors
    ///
    /// [`Refused`], with security on.
    #[cfg(feature = "enforce")]
    #[inline]
    pub fn decide<T: Request>(
        &self,
        who: &Principal,
        request: T,
        policy: &Policy,
    ) -> Result<Checked<T>, Refused> {
        if self.security == Security::On {
            self.enforce(who, &request, policy)?;
        }
        Ok(Checked {
            value: request,
            who: who.clone(),
        })
    }

    /// Built without feature `enforce`, this does nothing.
    ///
    /// # Errors
    ///
    /// Never.
    #[cfg(not(feature = "enforce"))]
    #[inline(always)]
    pub fn decide<T: Request>(
        &self,
        _who: &Principal,
        request: T,
        _policy: &Policy,
    ) -> Result<Checked<T>, Refused> {
        Ok(Checked { value: request })
    }

    #[cfg(feature = "enforce")]
    fn enforce<T: Request>(
        &self,
        who: &Principal,
        request: &T,
        policy: &Policy,
    ) -> Result<(), Refused> {
        if !policy.admits().contains(who.class()) {
            return Err(Refused::NotAdmitted);
        }
        if let Principal::Operator(key) = who
            && policy.needs_grant()
            && !self.grants.operator_may(key, request.what())
        {
            return Err(Refused::NotGranted);
        }
        Ok(())
    }
}

#[cfg(all(test, feature = "enforce"))]
mod tests {
    use super::*;
    use crate::Admits;

    #[derive(Debug)]
    struct Ping;

    impl Request for Ping {
        fn what(&self) -> What<'_> {
            What::Read("ping")
        }
    }

    const KEY: KeyId = KeyId([7; 32]);
    const ROOT: Principal = Principal::Local { uid: 0, gid: 0 };
    const USER: Principal = Principal::Local {
        uid: 1000,
        gid: 1000,
    };

    struct NoGrants;

    impl Grants for NoGrants {
        fn operator_may(&self, _key: &KeyId, _what: What<'_>) -> bool {
            false
        }
    }

    struct Unreachable;

    impl Grants for Unreachable {
        fn operator_may(&self, _key: &KeyId, _what: What<'_>) -> bool {
            panic!("security off must not look at grants");
        }
    }

    #[test]
    fn policy_decides_by_class() {
        let check = Check::new(Security::On);
        let root_only = Policy::new(Admits::ROOT);
        assert!(check.decide(&ROOT, Ping, &root_only).is_ok());
        assert_eq!(
            check.decide(&USER, Ping, &root_only).unwrap_err(),
            Refused::NotAdmitted
        );
        let local = Policy::new(Admits::LOCAL);
        assert!(check.decide(&USER, Ping, &local).is_ok());
        assert_eq!(
            check
                .decide(&Principal::Anonymous, Ping, &local)
                .unwrap_err(),
            Refused::NotAdmitted
        );
    }

    #[test]
    fn an_operator_also_needs_the_grant() {
        let policy = Policy::new(Admits::OPERATOR);
        let who = Principal::Operator(KEY);
        assert!(Check::new(Security::On).decide(&who, Ping, &policy).is_ok());
        let check = Check::with_grants(Security::On, &NoGrants);
        assert_eq!(
            check.decide(&who, Ping, &policy).unwrap_err(),
            Refused::NotGranted
        );
    }

    #[test]
    fn a_policy_can_let_an_operator_through_without_a_grant() {
        let check = Check::with_grants(Security::On, &NoGrants);
        let who = Principal::Operator(KEY);
        let open = Policy::new(Admits::ANYONE).without_grant();
        assert!(check.decide(&who, Ping, &open).is_ok());
        assert!(!open.needs_grant() && Policy::new(Admits::ANYONE).needs_grant());
        // It still has to be admitted: the grant is all that is waived.
        let peers = Policy::new(Admits::PEER).without_grant();
        assert_eq!(
            check.decide(&who, Ping, &peers).unwrap_err(),
            Refused::NotAdmitted
        );
        assert!(open.admits().contains(crate::PrincipalClass::Anonymous));
    }

    #[test]
    fn off_admits_everything_without_looking() {
        let check = Check::with_grants(Security::Off, &Unreachable);
        let who = Principal::Operator(KEY);
        let checked = check
            .decide(&who, Ping, &Policy::new(Admits::NOBODY))
            .unwrap();
        assert_eq!(checked.who(), &who);
    }

    #[test]
    fn nobody_means_nobody() {
        let check = Check::new(Security::On);
        assert!(
            check
                .decide(&ROOT, Ping, &Policy::new(Admits::NOBODY))
                .is_err()
        );
    }

    #[test]
    fn refusals_have_the_stable_code() {
        assert_eq!(Refused::NotAdmitted.error().code(), Error::Denied.code());
    }
}

#[cfg(all(test, not(feature = "enforce")))]
mod compiled_out {
    use super::*;
    use crate::Admits;

    #[derive(Debug)]
    struct Ping(u32);

    impl Request for Ping {
        fn what(&self) -> What<'_> {
            What::Read("ping")
        }
    }

    /// Nothing is left of the check: the switch is empty and a checked request is the request.
    #[test]
    fn the_check_compiles_to_nothing() {
        assert_eq!(size_of::<Check<'static>>(), 0);
        assert_eq!(size_of::<Checked<Ping>>(), size_of::<Ping>());
        let check = Check::new(Security::On);
        let checked = check
            .decide(&Principal::Anonymous, Ping(5), &Policy::new(Admits::NOBODY))
            .unwrap();
        assert_eq!(checked.get().0, 5);
        assert_eq!(checked.who(), &Principal::Anonymous);
    }
}
