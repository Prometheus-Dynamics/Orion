use core::fmt;

macro_rules! errors {
    ($($(#[$doc:meta])* $name:ident = $code:literal, $text:literal;)*) => {
        /// The one error type. Each case has a code that never changes, so a program in another
        /// language or on another build can branch on it.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum Error {
            $($(#[$doc])* $name,)*
        }

        impl Error {
            /// The stable code of this case. Codes are never reused.
            #[must_use]
            pub const fn code(self) -> u16 {
                match self { $(Self::$name => $code,)* }
            }

            /// The case with this code, or `None` for a code this build doesn't know.
            #[must_use]
            pub const fn from_code(code: u16) -> Option<Self> {
                match code { $($code => Some(Self::$name),)* _ => None }
            }

            /// A short description.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$name => $text,)* }
            }

            /// Every case, for tables and tests.
            pub const ALL: &'static [Self] = &[$(Self::$name,)*];
        }
    };
}

errors! {
    /// A name was empty.
    EmptyName = 1, "name is empty";
    /// A name was longer than its type allows.
    NameTooLong = 2, "name is too long";
    /// A name held a character its type doesn't allow.
    InvalidName = 3, "name holds a character that is not allowed";
    /// A remote clock was further ahead than the allowed drift.
    ClockSkew = 4, "remote clock is too far ahead";
    /// Bytes could not be decoded.
    Malformed = 5, "malformed data";
    /// An output buffer was too small.
    BufferTooSmall = 6, "buffer is too small";
    /// The other side doesn't support this.
    Unsupported = 7, "not supported";
    /// The named thing doesn't exist.
    NotFound = 8, "not found";
    /// The named thing already exists.
    AlreadyExists = 9, "already exists";
    /// The one check refused the request.
    Denied = 10, "denied";
    /// A bounded queue or resource was full.
    Busy = 11, "busy";
    /// A deadline passed.
    Timeout = 12, "timed out";
    /// The caller cancelled.
    Cancelled = 13, "cancelled";
    /// The target can't be reached right now.
    Unavailable = 14, "unavailable";
    /// A bug on the answering side.
    Internal = 15, "internal error";
    /// A digest didn't match.
    DigestMismatch = 16, "digest does not match";
    /// A request was out of the order the protocol requires.
    OutOfOrder = 17, "out of order";
    /// A run that carries on without its caller was asked for without a deadline.
    NoDeadline = 18, "a detached run needs a deadline";
    /// A deadline was longer than the node takes.
    DeadlineTooLong = 19, "deadline is longer than allowed";
    /// The node restarted a moment ago and what was asked for was there before it: the
    /// program that serves it has not come back yet. Asking again shortly may find it.
    Restoring = 20, "not back yet after a restart: ask again";
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl core::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_round_trip_and_are_unique() {
        for (i, a) in Error::ALL.iter().enumerate() {
            assert_eq!(Error::from_code(a.code()), Some(*a));
            for b in &Error::ALL[i + 1..] {
                assert_ne!(a.code(), b.code());
            }
        }
        assert_eq!(Error::from_code(0), None);
    }

    /// These numbers are on the wire. Changing one is a protocol break.
    #[test]
    fn codes_are_pinned() {
        assert_eq!(Error::EmptyName.code(), 1);
        assert_eq!(Error::Denied.code(), 10);
        assert_eq!(Error::OutOfOrder.code(), 17);
        assert_eq!(Error::Restoring.code(), 20);
    }
}
