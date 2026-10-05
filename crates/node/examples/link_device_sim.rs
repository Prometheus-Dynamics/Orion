//! A simulated microcontroller for trying the `link-gateway` without hardware.
//!
//! ```text
//! cargo run -p orion-node --example link_device_sim --features link-gateway
//! ```
//!
//! Without `--port`, it creates a pseudo-terminal and prints the path to give the node
//! (`ORION_NODE_LINKS=serial:<path>`); with `--port <path>` it opens an existing serial port
//! (for example one end of a `socat` pair or a USB-serial adapter looped to the gateway). The
//! device side is exactly what an MCU port runs: an `orion_link::device::StreamDevice` fed with
//! bytes and polled with a millisecond clock. It publishes one provider with one resource and
//! prints the leases it receives.
//!
//! Options: `--name <device>` (default `imu-board`), `--port <path>`, `--seconds <n>` (default:
//! run until killed).

use orion_link::Stream;
use orion_link::device::{DeviceConfig, DeviceEvent, StreamDevice};
use orion_link::message::{AvailabilityState, HealthState, NodeId, ProviderRecord, ResourceRecord};
use std::ffi::{CStr, CString};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::time::{Duration, Instant};

struct Args {
    name: String,
    port: Option<String>,
    seconds: Option<u64>,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        name: "imu-board".into(),
        port: None,
        seconds: None,
    };
    let mut iter = std::env::args().skip(1);
    while let Some(flag) = iter.next() {
        let mut value = || iter.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--name" => args.name = value()?,
            "--port" => args.port = Some(value()?),
            "--seconds" => {
                args.seconds = Some(value()?.parse().map_err(|_| "--seconds needs a number")?)
            }
            other => return Err(format!("unknown argument `{other}`")),
        }
    }
    Ok(args)
}

fn cvt(result: libc::c_int) -> io::Result<libc::c_int> {
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

/// Creates a pty; returns the master and the slave path for the node.
fn open_pty() -> io::Result<(OwnedFd, String)> {
    // SAFETY: plain libc calls; the master fd is owned right away and `name` is sized as passed.
    unsafe {
        let master = cvt(libc::posix_openpt(
            libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC,
        ))?;
        let fd = OwnedFd::from_raw_fd(master);
        cvt(libc::grantpt(master))?;
        cvt(libc::unlockpt(master))?;
        let mut name = [0 as libc::c_char; 128];
        cvt(libc::ptsname_r(master, name.as_mut_ptr(), name.len()))?;
        let path = CStr::from_ptr(name.as_ptr()).to_string_lossy().into_owned();
        Ok((fd, path))
    }
}

/// Opens an existing serial port in raw mode, 115200 8N1.
fn open_port(path: &str) -> io::Result<OwnedFd> {
    let c_path = CString::new(path).map_err(|_| io::Error::other("path contains NUL"))?;
    // SAFETY: `c_path` is NUL-terminated; the fd is owned right away; `tio` is a valid termios.
    unsafe {
        let raw = cvt(libc::open(
            c_path.as_ptr(),
            libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC,
        ))?;
        let fd = OwnedFd::from_raw_fd(raw);
        let mut tio: libc::termios = std::mem::zeroed();
        cvt(libc::tcgetattr(raw, &mut tio))?;
        libc::cfmakeraw(&mut tio);
        cvt(libc::cfsetispeed(&mut tio, libc::B115200))?;
        cvt(libc::cfsetospeed(&mut tio, libc::B115200))?;
        cvt(libc::tcsetattr(raw, libc::TCSANOW, &tio))?;
        Ok(fd)
    }
}

fn set_nonblocking(fd: &OwnedFd) -> io::Result<()> {
    // SAFETY: fcntl on an open fd.
    unsafe {
        let flags = cvt(libc::fcntl(fd.as_raw_fd(), libc::F_GETFL))?;
        cvt(libc::fcntl(
            fd.as_raw_fd(),
            libc::F_SETFL,
            flags | libc::O_NONBLOCK,
        ))?;
    }
    Ok(())
}

fn main() -> io::Result<()> {
    let args = parse_args().map_err(io::Error::other)?;
    let fd = match &args.port {
        Some(path) => open_port(path)?,
        None => {
            let (master, slave) = open_pty()?;
            println!("link_device_sim: pty {slave}");
            println!("  run the node with ORION_NODE_LINKS=serial:{slave}");
            master
        }
    };
    set_nonblocking(&fd)?;

    let provider_id = format!("provider.{}", args.name);
    let provider = ProviderRecord::builder(
        orion_link::message::ProviderId::new(provider_id.clone()),
        NodeId::new("assigned-by-gateway"),
    )
    .resource_type("imu.sample_source")
    .build();
    let resource = ResourceRecord::builder(
        orion_link::message::ResourceId::new(format!("{}.imu-0", args.name)),
        "imu.sample_source",
        orion_link::message::ProviderId::new(provider_id),
    )
    .health(HealthState::Healthy)
    .availability(AvailabilityState::Available)
    .label("rate=100hz")
    .build();

    let mut device =
        StreamDevice::<512, 512, String>::new(DeviceConfig::provider(args.name.clone()), Stream);
    device
        .publish_provider_state(&provider, &[resource])
        .map_err(|error| io::Error::other(format!("{error:?}")))?;

    let started = Instant::now();
    let mut rx = [0u8; 512];
    let mut tx = [0u8; 256];
    loop {
        if args
            .seconds
            .is_some_and(|seconds| started.elapsed() >= Duration::from_secs(seconds))
        {
            return Ok(());
        }
        // SAFETY: `rx` is valid for `rx.len()` bytes.
        let n = unsafe { libc::read(fd.as_raw_fd(), rx.as_mut_ptr().cast(), rx.len()) };
        if n > 0 {
            device.receive(rx.get(..n.unsigned_abs()).unwrap_or_default());
        }
        let now_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        device.poll(now_ms);
        while let Some(event) = device.next_event() {
            match event {
                DeviceEvent::LeasesChanged => {
                    let held: Vec<String> = device
                        .leases()
                        .map(|lease| {
                            format!(
                                "{} -> {}",
                                lease.resource_id,
                                lease.holder_workload_id.unwrap_or("-")
                            )
                        })
                        .collect();
                    println!("{now_ms:>7} ms  leases {held:?}");
                }
                other => println!("{now_ms:>7} ms  {other:?}"),
            }
        }
        loop {
            let len = device.transmit(&mut tx);
            if len == 0 {
                break;
            }
            let mut out = tx.get(..len).unwrap_or_default();
            while !out.is_empty() {
                // SAFETY: `out` is valid for `out.len()` bytes.
                let written =
                    unsafe { libc::write(fd.as_raw_fd(), out.as_ptr().cast(), out.len()) };
                if written > 0 {
                    out = out.get(written.unsigned_abs()..).unwrap_or_default();
                } else {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}
