//! Process-local monotonic time in microseconds, the time base of NT4 timestamps before sync.

use std::sync::OnceLock;
use std::time::Instant;

static EPOCH: OnceLock<Instant> = OnceLock::new();

/// Microseconds since the first call in this process (monotonic, never negative).
///
/// NT4 timestamps are microseconds on the server's clock. A client learns the offset from the
/// server's clock with RTT sync (see `ClientHandle::server_time_us`).
pub fn now_micros() -> i64 {
    let epoch = EPOCH.get_or_init(Instant::now);
    i64::try_from(epoch.elapsed().as_micros()).unwrap_or(i64::MAX)
}
