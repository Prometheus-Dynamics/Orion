use std::fs::File;
use std::io::{ErrorKind, Read, Seek, Write};
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::net::UnixStream;

use orion_fdpass::{MAX_FDS, RecvError, SendError, recv, send, send_on};

fn open_fds() -> usize {
    std::fs::read_dir("/proc/self/fd").unwrap().count()
}

fn memfile(text: &[u8]) -> File {
    let path = std::env::temp_dir().join(format!(
        "orion-fdpass-{}-{:?}-{}",
        std::process::id(),
        std::thread::current().id(),
        text.len()
    ));
    let mut file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    std::fs::remove_file(path).unwrap();
    file.write_all(text).unwrap();
    file.rewind().unwrap();
    file
}

fn read_all(fd: OwnedFd) -> Vec<u8> {
    let mut file = File::from(fd);
    let mut out = Vec::new();
    file.read_to_end(&mut out).unwrap();
    out
}

// All in one test, so nothing else opens a descriptor while the counts are compared.
#[test]
fn frames_carry_their_descriptors_and_nothing_leaks() {
    let before = open_fds();
    a_frame_with_descriptors();
    each_set_stays_with_its_frame();
    gathered_parts_arrive_joined();
    a_truncated_control_message_closes_what_arrived();
    too_many_descriptors_are_refused_on_send();
    a_frame_too_large_for_the_buffer_is_refused();
    a_partial_frame_stays_queued();
    a_closed_peer_is_none();
    a_peer_closing_inside_a_frame_is_short();
    received_descriptors_are_close_on_exec();
    assert_eq!(open_fds(), before, "descriptors leaked");
}

fn a_frame_with_descriptors() {
    let (a, b) = UnixStream::pair().unwrap();
    let (one, two) = (memfile(b"first"), memfile(b"second!"));
    send_on(&a, &[b"meta"], &[one.as_fd(), two.as_fd()]).unwrap();
    let mut buf = [0; 16];
    let mut got = recv(b.as_fd(), &mut buf, MAX_FDS, false).unwrap().unwrap();
    assert_eq!(&buf[..got.len], b"meta");
    assert_eq!(got.fds.len(), 2);
    assert_eq!(read_all(got.fds.take(0).unwrap()), b"first");
    assert_eq!(read_all(got.fds.take(1).unwrap()), b"second!");
    assert!(got.fds.take(0).is_none());
}

fn each_set_stays_with_its_frame() {
    let (a, b) = UnixStream::pair().unwrap();
    let file = memfile(b"x");
    send_on(&a, &[b"none"], &[]).unwrap();
    send_on(&a, &[b"one"], &[file.as_fd()]).unwrap();
    send_on(&a, &[b"none"], &[]).unwrap();
    let mut buf = [0; 16];
    let lens: Vec<usize> = (0..3)
        .map(|_| {
            recv(b.as_fd(), &mut buf, MAX_FDS, false)
                .unwrap()
                .unwrap()
                .fds
                .len()
        })
        .collect();
    assert_eq!(lens, [0, 1, 0]);
}

fn gathered_parts_arrive_joined() {
    let (a, b) = UnixStream::pair().unwrap();
    send(a.as_fd(), &[b"ab", b"", b"cde"], &[], false).unwrap();
    let mut buf = [0; 16];
    let got = recv(b.as_fd(), &mut buf, 0, false).unwrap().unwrap();
    assert_eq!(&buf[..got.len], b"abcde");
}

fn a_truncated_control_message_closes_what_arrived() {
    let (a, b) = UnixStream::pair().unwrap();
    let files = [memfile(b"1"), memfile(b"22"), memfile(b"333")];
    let during = open_fds();
    let borrowed: Vec<_> = files.iter().map(AsFd::as_fd).collect();
    send_on(&a, &[b"m"], &borrowed).unwrap();
    let mut buf = [0; 8];
    let err = recv(b.as_fd(), &mut buf, 1, false).unwrap_err();
    assert!(matches!(err, RecvError::TruncatedControl), "{err}");
    assert_eq!(
        open_fds(),
        during,
        "a truncated frame left a descriptor open"
    );
}

fn too_many_descriptors_are_refused_on_send() {
    let (a, _b) = UnixStream::pair().unwrap();
    let file = memfile(b"x");
    let fds = vec![file.as_fd(); MAX_FDS + 1];
    assert!(matches!(
        send_on(&a, &[b"m"], &fds),
        Err(SendError::TooLarge)
    ));
}

fn a_frame_too_large_for_the_buffer_is_refused() {
    let (a, b) = UnixStream::pair().unwrap();
    send_on(&a, &[b"too long"], &[]).unwrap();
    let mut buf = [0; 4];
    let err = recv(b.as_fd(), &mut buf, 0, false).unwrap_err();
    assert!(matches!(err, RecvError::TooLarge { len: 8 }), "{err}");
}

fn a_partial_frame_stays_queued() {
    let (mut a, b) = UnixStream::pair().unwrap();
    let mut buf = [0; 8];
    let err = recv(b.as_fd(), &mut buf, 0, true).unwrap_err();
    assert!(
        matches!(&err, RecvError::Io(e) if e.kind() == ErrorKind::WouldBlock),
        "{err}"
    );
    a.write_all(&4_u32.to_le_bytes()).unwrap();
    a.write_all(b"ab").unwrap();
    let err = recv(b.as_fd(), &mut buf, 0, true).unwrap_err();
    assert!(
        matches!(&err, RecvError::Io(e) if e.kind() == ErrorKind::WouldBlock),
        "{err}"
    );
    a.write_all(b"cd").unwrap();
    let got = recv(b.as_fd(), &mut buf, 0, true).unwrap().unwrap();
    assert_eq!(&buf[..got.len], b"abcd");
}

fn a_closed_peer_is_none() {
    let (a, b) = UnixStream::pair().unwrap();
    drop(a);
    let mut buf = [0; 8];
    assert!(recv(b.as_fd(), &mut buf, 0, false).unwrap().is_none());
}

fn a_peer_closing_inside_a_frame_is_short() {
    let (mut a, b) = UnixStream::pair().unwrap();
    a.write_all(&4_u32.to_le_bytes()).unwrap();
    a.write_all(b"ab").unwrap();
    drop(a);
    let mut buf = [0; 8];
    let err = recv(b.as_fd(), &mut buf, 0, false).unwrap_err();
    assert!(matches!(err, RecvError::ShortRead), "{err}");
}

fn received_descriptors_are_close_on_exec() {
    use std::os::fd::AsRawFd;
    let (a, b) = UnixStream::pair().unwrap();
    let file = memfile(b"x");
    send_on(&a, &[b"m"], &[file.as_fd()]).unwrap();
    let mut buf = [0; 8];
    let mut got = recv(b.as_fd(), &mut buf, 1, false).unwrap().unwrap();
    let fd = got.fds.take(0).unwrap();
    // SAFETY: F_GETFD only reads the flags of a descriptor this test owns.
    let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFD) };
    assert_eq!(flags & libc::FD_CLOEXEC, libc::FD_CLOEXEC);
}
