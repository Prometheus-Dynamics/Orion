//! Credentials of the process at the other end of a Unix stream socket.

use std::os::fd::AsRawFd;

use tokio::net::UnixStream;

use crate::UnixPeerIdentity;

/// The peer's pid, uid and primary gid (`SO_PEERCRED`), plus its supplementary groups where the
/// kernel reports them (`SO_PEERGROUPS`, Linux 4.13 and newer). The credentials are those of the
/// process that connected, taken when it connected. `None` when the platform reports no
/// credentials for the socket.
pub fn unix_peer_identity(stream: &UnixStream) -> Option<UnixPeerIdentity> {
    let cred = stream.peer_cred().ok()?;
    Some(UnixPeerIdentity {
        pid: cred.pid().and_then(|pid| u32::try_from(pid).ok()),
        uid: cred.uid(),
        gid: cred.gid(),
        groups: supplementary_groups(stream.as_raw_fd()),
    })
}

/// Most supplementary groups read from a peer (the kernel's `NGROUPS_MAX` is 65536; real
/// processes have a handful).
#[cfg(any(target_os = "linux", target_os = "android"))]
const MAX_PEER_GROUPS: usize = 4096;

#[cfg(any(target_os = "linux", target_os = "android"))]
fn supplementary_groups(fd: std::os::fd::RawFd) -> Vec<u32> {
    // gid_t is u32 on Linux and Android.
    let mut groups: Vec<u32> = vec![0; 32];
    loop {
        let mut len = libc::socklen_t::try_from(groups.len() * size_of::<libc::gid_t>())
            .unwrap_or(libc::socklen_t::MAX);
        // SAFETY: `groups` is a live, writable buffer of `len` bytes, and `len` points to a
        // socklen_t the kernel updates with the number of bytes written (or needed).
        let rc = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_PEERGROUPS,
                groups.as_mut_ptr().cast(),
                &mut len,
            )
        };
        let count = len as usize / size_of::<libc::gid_t>();
        if rc == 0 {
            groups.truncate(count.min(groups.len()));
            return groups;
        }
        let too_small = std::io::Error::last_os_error().raw_os_error() == Some(libc::ERANGE);
        if !too_small || count <= groups.len() || count > MAX_PEER_GROUPS {
            // Older kernels (ENOPROTOOPT) or an oversized group list: primary group only.
            return Vec::new();
        }
        groups.resize(count, 0);
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn supplementary_groups(_fd: std::os::fd::RawFd) -> Vec<u32> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reports_this_process_as_the_peer_of_a_socket_pair() {
        let (left, _right) = UnixStream::pair().expect("socket pair");
        let identity = unix_peer_identity(&left).expect("peer credentials");
        // SAFETY: getuid/getgid have no preconditions.
        let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
        assert_eq!(identity.uid, uid);
        assert_eq!(identity.gid, gid);
        assert!(identity.is_member_of(gid));
        #[cfg(target_os = "linux")]
        {
            // SAFETY: a zero-length query returns the number of supplementary groups.
            let count = unsafe { libc::getgroups(0, std::ptr::null_mut()) };
            let mut own = vec![0; usize::try_from(count).unwrap_or(0)];
            // SAFETY: `own` holds `count` gid_t slots.
            let written = unsafe { libc::getgroups(count, own.as_mut_ptr()) };
            own.truncate(usize::try_from(written).unwrap_or(0));
            for group in own {
                assert!(
                    identity.is_member_of(group),
                    "supplementary group {group} missing from {identity:?}"
                );
            }
        }
    }
}
