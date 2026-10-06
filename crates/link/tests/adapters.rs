//! Tests for the optional `embedded-io`, `embedded-io-async`, and `embedded-can` adapters.

#![cfg(any(
    feature = "embedded-io",
    feature = "embedded-io-async",
    feature = "embedded-can"
))]

mod common;

#[allow(unused_imports)]
use common::stream_bytes;
#[allow(unused_imports)]
use orion_link::{FrameHeader, StreamDecoder, StreamEncoder};

/// In-memory writer/reader implementing the embedded-io traits without relying on their `std`
/// or `alloc` impls.
#[cfg(any(feature = "embedded-io", feature = "embedded-io-async"))]
struct Pipe {
    data: Vec<u8>,
    pos: usize,
    /// Largest read/fill size, to exercise partial reads.
    chunk: usize,
}

#[cfg(any(feature = "embedded-io", feature = "embedded-io-async"))]
impl Pipe {
    fn new(data: Vec<u8>, chunk: usize) -> Self {
        Self {
            data,
            pos: 0,
            chunk,
        }
    }

    fn take(&mut self, buf: &mut [u8]) -> usize {
        let n = buf.len().min(self.chunk).min(self.data.len() - self.pos);
        buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        n
    }

    fn available(&self) -> &[u8] {
        let end = (self.pos + self.chunk).min(self.data.len());
        &self.data[self.pos..end]
    }
}

// `embedded_io_async::ErrorType` is a re-export of the same trait.
#[cfg(feature = "embedded-io")]
use embedded_io::ErrorType;
#[cfg(all(feature = "embedded-io-async", not(feature = "embedded-io")))]
use embedded_io_async::ErrorType;

#[cfg(any(feature = "embedded-io", feature = "embedded-io-async"))]
impl ErrorType for Pipe {
    type Error = core::convert::Infallible;
}

#[cfg(feature = "embedded-io")]
mod blocking {
    use super::*;
    use orion_link::ReadFrameError;
    use orion_link::io::{read_frame, read_frame_buffered, write_stream};

    impl embedded_io::Read for Pipe {
        fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
            Ok(self.take(buf))
        }
    }

    impl embedded_io::BufRead for Pipe {
        fn fill_buf(&mut self) -> Result<&[u8], Self::Error> {
            Ok(self.available())
        }

        fn consume(&mut self, amt: usize) {
            self.pos += amt;
        }
    }

    impl embedded_io::Write for Pipe {
        fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
            let n = buf.len().min(self.chunk);
            self.data.extend_from_slice(&buf[..n]);
            Ok(n)
        }

        fn flush(&mut self) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[test]
    fn write_then_read_frames() {
        let payload = vec![0u8; 100];
        let mut pipe = Pipe::new(Vec::new(), 7);
        write_stream(
            &mut pipe,
            StreamEncoder::for_message(FrameHeader::new(1, 1), &payload),
        )
        .unwrap();
        write_stream(
            &mut pipe,
            StreamEncoder::for_message(FrameHeader::new(2, 2), b"two"),
        )
        .unwrap();
        assert_eq!(
            &pipe.data[..],
            &[
                stream_bytes(FrameHeader::new(1, 1), &payload),
                stream_bytes(FrameHeader::new(2, 2), b"two")
            ]
            .concat()[..]
        );

        let mut decoder = StreamDecoder::<256>::new();
        let frame = read_frame(&mut pipe, &mut decoder).unwrap();
        assert_eq!((frame.kind(), frame.payload()), (1, &payload[..]));
        let frame = read_frame(&mut pipe, &mut decoder).unwrap();
        assert_eq!((frame.kind(), frame.payload()), (2, &b"two"[..]));
        assert_eq!(
            read_frame(&mut pipe, &mut decoder),
            Err(ReadFrameError::Eof)
        );
    }

    #[test]
    fn buffered_read_leaves_following_bytes_and_skips_corruption() {
        let mut wire = vec![0x42, 0x43, 0x00]; // a corrupt packet first
        wire.extend(stream_bytes(FrameHeader::new(1, 1), b"one"));
        wire.extend(stream_bytes(FrameHeader::new(2, 2), b"two"));
        let mut pipe = Pipe::new(wire, 64);
        let mut decoder = StreamDecoder::<64>::new();
        let frame = read_frame_buffered(&mut pipe, &mut decoder).unwrap();
        assert_eq!(frame.payload(), b"one");
        let frame = read_frame_buffered(&mut pipe, &mut decoder).unwrap();
        assert_eq!(frame.payload(), b"two");
        assert_eq!(decoder.stats().framing_errors, 1);
        assert_eq!(
            read_frame_buffered(&mut pipe, &mut decoder),
            Err(ReadFrameError::Eof)
        );
    }
}

#[cfg(feature = "embedded-io-async")]
mod nonblocking {
    use super::*;
    use core::future::Future;
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};
    use orion_link::io_async::{read_frame, read_frame_buffered, write_stream};

    fn block_on<F: Future>(future: F) -> F::Output {
        let mut future = pin!(future);
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                return output;
            }
        }
    }

    impl embedded_io_async::Read for Pipe {
        async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
            Ok(self.take(buf))
        }
    }

    impl embedded_io_async::BufRead for Pipe {
        async fn fill_buf(&mut self) -> Result<&[u8], Self::Error> {
            Ok(self.available())
        }

        fn consume(&mut self, amt: usize) {
            self.pos += amt;
        }
    }

    impl embedded_io_async::Write for Pipe {
        async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
            let n = buf.len().min(self.chunk);
            self.data.extend_from_slice(&buf[..n]);
            Ok(n)
        }

        async fn flush(&mut self) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[test]
    fn async_write_then_read_frames() {
        block_on(async {
            let mut pipe = Pipe::new(Vec::new(), 5);
            write_stream(
                &mut pipe,
                StreamEncoder::for_message(FrameHeader::new(3, 3), b"async"),
            )
            .await
            .unwrap();
            write_stream(
                &mut pipe,
                StreamEncoder::for_message(FrameHeader::new(4, 4), b"more"),
            )
            .await
            .unwrap();
            let mut decoder = StreamDecoder::<64>::new();
            let frame = read_frame(&mut pipe, &mut decoder).await.unwrap();
            assert_eq!(frame.payload(), b"async");
            let frame = read_frame_buffered(&mut pipe, &mut decoder).await.unwrap();
            assert_eq!(frame.payload(), b"more");
            assert!(read_frame(&mut pipe, &mut decoder).await.is_err());
        });
    }
}

#[cfg(feature = "embedded-can")]
mod can {
    use super::*;
    use embedded_can::{ExtendedId, Frame, Id, StandardId};
    use orion_link::{CanLinkIds, Reassembler, SegmentMtu, Segmenter};

    /// Minimal CAN FD-capable frame type for tests.
    #[derive(Debug, Clone)]
    struct TestFrame {
        id: Id,
        data: Vec<u8>,
        remote: bool,
    }

    impl Frame for TestFrame {
        fn new(id: impl Into<Id>, data: &[u8]) -> Option<Self> {
            (data.len() <= 64).then(|| Self {
                id: id.into(),
                data: data.to_vec(),
                remote: false,
            })
        }

        fn new_remote(id: impl Into<Id>, dlc: usize) -> Option<Self> {
            Some(Self {
                id: id.into(),
                data: vec![0; dlc],
                remote: true,
            })
        }

        fn is_extended(&self) -> bool {
            matches!(self.id, Id::Extended(_))
        }

        fn is_remote_frame(&self) -> bool {
            self.remote
        }

        fn id(&self) -> Id {
            self.id
        }

        fn dlc(&self) -> usize {
            self.data.len()
        }

        fn data(&self) -> &[u8] {
            &self.data
        }
    }

    #[test]
    fn ids_convert_to_embedded_can() {
        let ids = CanLinkIds::new(0x101, 0x181, false);
        assert_eq!(
            ids.device_to_host_id(),
            StandardId::new(0x101).map(Id::Standard)
        );
        assert_eq!(
            ids.host_to_device_id(),
            StandardId::new(0x181).map(Id::Standard)
        );
        assert!(ids.is_device_to_host(Id::Standard(StandardId::new(0x101).unwrap())));
        assert!(!ids.is_host_to_device(Id::Standard(StandardId::new(0x101).unwrap())));
        let ext = CanLinkIds::new(0x1234_5678, 0x1234_5679, true);
        assert_eq!(
            ext.device_to_host_id(),
            ExtendedId::new(0x1234_5678).map(Id::Extended)
        );
        assert_eq!(CanLinkIds::new(0x800, 0x1, false).device_to_host_id(), None);
    }

    #[test]
    fn segments_round_trip_through_can_frames() {
        let ids = CanLinkIds::for_address(CanLinkIds::new(0x100, 0x180, false), 3).unwrap();
        let id = ids.device_to_host_id().unwrap();
        let payload: Vec<u8> = (0..200).map(|i| i as u8).collect();
        let mut rx = Reassembler::<256>::new();
        let mut delivered = None;
        // A stray remote frame on the same identifier is ignored.
        let remote = TestFrame::new_remote(id, 0).unwrap();
        assert_eq!(rx.push_can_frame(&remote), Ok(None));
        for segment in Segmenter::for_message(FrameHeader::new(5, 6), &payload, SegmentMtu::FD) {
            let frame: TestFrame = segment.to_can_frame(id).unwrap();
            assert!(ids.is_device_to_host(frame.id()));
            if let Some(view) = rx.push_can_frame(&frame).unwrap() {
                delivered = Some(view.payload().to_vec());
            }
        }
        assert_eq!(delivered, Some(payload));
    }
}
