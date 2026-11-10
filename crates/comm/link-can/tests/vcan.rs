//! The kernel's CAN sockets over a virtual bus. Needs the interface `vcan0`
//! (`modprobe vcan && ip link add vcan0 type vcan && ip link set vcan0 mtu 72 up`).
//! Where there is none, this is not tested and says so.
#![cfg(feature = "socketcan")]

use std::time::Duration;

use orion_link_can::{CanBus, CanCarrier, CanFrame, CanIo, OpenCan, SocketCanBus};
use orion_msg_session::{Hello, Ping, Pong};
use orion_server::{Comm, Limits, Roles, Security};
use orion_wire::{CanLinkIds, Decode, Reassembler, SegmentMtu, Segmenter, encode_body, kind};

const BASE: CanLinkIds = CanLinkIds::new(0x100, 0x180, false);

#[test]
fn the_wire_crosses_a_virtual_can_bus() {
    if !std::path::Path::new("/sys/class/net/vcan0").exists() {
        eprintln!("UNTESTED: no vcan0 interface on this machine");
        return;
    }
    for fd in [false, true] {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        tokio::task::LocalSet::new().block_on(&runtime, async move {
            let comm = Comm::new(
                "board".parse().expect("name"),
                Security::Off,
                None,
                Limits::default(),
            );
            let bus = CanBus {
                name: "vcan0".into(),
                interface: "vcan0".into(),
                base: BASE,
                addresses: vec![3],
                fd,
            };
            let carrier = CanCarrier::new(
                &bus,
                SocketCanBus {
                    interface: "vcan0".into(),
                    fd,
                },
            )
            .expect("bus");
            let stats = carrier.stats();
            let serving = comm.clone();
            tokio::task::spawn_local(async move { serving.serve(carrier).await });

            // The device's end is a second socket on the same bus, listening for what the
            // board sends to address 3.
            let ids = CanLinkIds::for_address(BASE, 3).expect("ids");
            let listen = CanLinkIds::new(ids.host_to_device, ids.device_to_host, false);
            let device = SocketCanBus {
                interface: "vcan0".into(),
                fd,
            }
            .open(&[listen])
            .await
            .expect("device socket");
            tokio::time::sleep(Duration::from_millis(50)).await;

            let mtu = if fd {
                SegmentMtu::FD
            } else {
                SegmentMtu::CLASSIC
            };
            let mut reassembler = Box::new(Reassembler::<1024>::new());
            let mut seq = 0;
            for round in 0..200_u64 {
                let mut frame = [0_u8; 256];
                seq += 1;
                let len = if round == 0 {
                    let hello = Hello {
                        name: "sensor",
                        roles: Roles::DEVICE,
                        max_frame: 1024,
                        revision: 1,
                    };
                    encode_body(&hello, seq, &mut frame)
                } else {
                    encode_body(&Ping { now_ms: round }, seq, &mut frame)
                }
                .expect("encode");
                let mut segments = Segmenter::for_frame(&frame[..len], mtu);
                let mut out = CanFrame {
                    id: ids.device_to_host,
                    extended: false,
                    len: 0,
                    data: [0; 64],
                };
                while let Some(len) = segments.next_into(&mut out.data).expect("segment") {
                    out.len = len as u8;
                    device.send(&out).await.expect("send");
                }
                let want = if round == 0 {
                    kind::WELCOME
                } else {
                    kind::PONG
                };
                loop {
                    let segment = tokio::time::timeout(Duration::from_secs(2), device.recv())
                        .await
                        .expect("an answer")
                        .expect("recv");
                    if let Ok(Some(answer)) = reassembler.push(segment.bytes())
                        && answer.kind() == want
                    {
                        if round > 0 {
                            assert_eq!(Pong::decode(answer.payload()).expect("pong").now_ms, round);
                        }
                        break;
                    }
                }
            }
            let stats = stats();
            assert_eq!(
                (stats.connections, stats.frames_rx, stats.broken),
                (1, 200, 0)
            );
        });
    }
}
