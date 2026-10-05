//! Fixed-layout control-protocol preamble for local IPC.
//!
//! Every local control message starts with a 4-byte preamble that is *not* rkyv-encoded, so its
//! layout never changes between Orion builds:
//!
//! ```text
//! [magic b'O' b'C'][CONTROL_PROTOCOL_VERSION u16 LE]
//! ```
//!
//! Unary exchanges (`UnixControlClient` / `UnixControlServer`) send `[preamble][archive]` until
//! EOF in each direction. Stream frames are `[preamble][payload_len u32 LE][archive]`. Readers
//! validate the preamble before touching the archive, so a version skew surfaces as
//! [`IpcTransportError::ProtocolMismatch`] instead of an rkyv decode error. On mismatch a server
//! answers with a payload-free message carrying its own preamble so the client learns the remote
//! version and reports the same typed error.

use orion_core::{CONTROL_PROTOCOL_VERSION, ControlProtocolMismatch};

use crate::IpcTransportError;

/// Magic bytes identifying an Orion control-protocol message.
pub const CONTROL_PREAMBLE_MAGIC: [u8; 2] = *b"OC";
/// Size of the fixed control-protocol preamble.
pub const CONTROL_PREAMBLE_BYTES: usize = 4;

const LOCAL_PREAMBLE: [u8; CONTROL_PREAMBLE_BYTES] = {
    let version = CONTROL_PROTOCOL_VERSION.to_le_bytes();
    [
        CONTROL_PREAMBLE_MAGIC[0],
        CONTROL_PREAMBLE_MAGIC[1],
        version[0],
        version[1],
    ]
};

/// Preamble announcing this build's [`CONTROL_PROTOCOL_VERSION`].
pub const fn control_preamble() -> [u8; CONTROL_PREAMBLE_BYTES] {
    LOCAL_PREAMBLE
}

/// Validates a received preamble: magic first, then the protocol version.
pub fn check_control_preamble(preamble: &[u8]) -> Result<(), IpcTransportError> {
    if preamble.len() < CONTROL_PREAMBLE_BYTES || preamble[..2] != CONTROL_PREAMBLE_MAGIC {
        return Err(IpcTransportError::DecodeFailed(
            "missing Orion control protocol preamble; the peer is not an Orion control client \
             or was built from an incompatible Orion release"
                .into(),
        ));
    }
    ControlProtocolMismatch::check(u16::from_le_bytes([preamble[2], preamble[3]]))
        .map_err(IpcTransportError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_preamble_roundtrips() {
        assert_eq!(check_control_preamble(&control_preamble()), Ok(()));
    }

    #[test]
    fn version_skew_is_a_typed_mismatch() {
        let mut preamble = control_preamble();
        preamble[2..].copy_from_slice(&(CONTROL_PROTOCOL_VERSION + 7).to_le_bytes());
        assert_eq!(
            check_control_preamble(&preamble),
            Err(IpcTransportError::ProtocolMismatch {
                local: CONTROL_PROTOCOL_VERSION,
                remote: CONTROL_PROTOCOL_VERSION + 7,
            })
        );
    }

    fn skewed_preamble() -> [u8; CONTROL_PREAMBLE_BYTES] {
        let mut preamble = control_preamble();
        preamble[2..].copy_from_slice(&(CONTROL_PROTOCOL_VERSION + 1).to_le_bytes());
        preamble
    }

    #[cfg(unix)]
    fn socket_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("orion-preamble-{name}-{}.sock", std::process::id()))
    }

    fn envelope() -> crate::ControlEnvelope {
        crate::ControlEnvelope {
            source: crate::LocalAddress::new("orionctl"),
            destination: crate::LocalAddress::new("orion"),
            message: orion_control_plane::ControlMessage::QueryObservability,
        }
    }

    const EXPECTED_MISMATCH: IpcTransportError = IpcTransportError::ProtocolMismatch {
        local: CONTROL_PROTOCOL_VERSION,
        remote: CONTROL_PROTOCOL_VERSION + 1,
    };

    #[tokio::test]
    async fn stream_reader_rejects_skewed_frame_before_decoding_and_skips_it() {
        use tokio::io::AsyncWriteExt;

        let (mut client, mut server) = tokio::io::duplex(1024);
        // A "future" frame whose archive is not decodable by this build.
        client.write_all(&skewed_preamble()).await.expect("write");
        client.write_u32_le(3).await.expect("write");
        client.write_all(b"xyz").await.expect("write");
        crate::write_control_frame(&mut client, &envelope())
            .await
            .expect("write");

        let err = crate::read_control_frame(&mut server)
            .await
            .expect_err("skewed frame should be rejected");
        assert_eq!(err, EXPECTED_MISMATCH);
        assert!(err.to_string().contains("upgrade orionctl"));
        // The skewed payload was drained, so the stream stays framed.
        let next = crate::read_control_frame(&mut server)
            .await
            .expect("next frame should decode")
            .expect("next frame should exist");
        assert_eq!(next, envelope());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stream_client_reports_mismatch_frame_from_server() {
        let path = socket_path("stream-client");
        let _ = std::fs::remove_file(&path);
        let listener = tokio::net::UnixListener::bind(&path).expect("bind");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let err = crate::read_control_frame(&mut stream)
                .await
                .expect("hello should decode")
                .map(|_| ());
            assert_eq!(err, Some(()));
            // Pretend to be an orion-node on another protocol version.
            use tokio::io::AsyncWriteExt;
            stream.write_all(&skewed_preamble()).await.expect("write");
            stream.write_u32_le(0).await.expect("write");
        });

        let mut client = crate::UnixControlStreamClient::connect(&path)
            .await
            .expect("connect");
        client.send(&envelope()).await.expect("send");
        let err = client.recv().await.expect_err("mismatch should surface");
        assert_eq!(err, EXPECTED_MISMATCH);
        server.await.expect("server");
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unary_server_answers_skewed_client_with_its_preamble() {
        use std::sync::Arc;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        struct Echo;
        impl crate::UnixControlHandler for Echo {
            fn handle_control(
                &self,
                envelope: crate::ControlEnvelope,
            ) -> Result<crate::ControlEnvelope, IpcTransportError> {
                Ok(envelope)
            }
        }

        let path = socket_path("unary-server");
        let server = crate::UnixControlServer::bind(&path, Arc::new(Echo))
            .await
            .expect("bind");
        let task = tokio::spawn(server.serve());

        let mut raw = tokio::net::UnixStream::connect(&path)
            .await
            .expect("connect");
        raw.write_all(&skewed_preamble()).await.expect("write");
        raw.write_all(b"archive from another release")
            .await
            .expect("write");
        raw.shutdown().await.expect("shutdown");
        let mut response = Vec::new();
        raw.read_to_end(&mut response).await.expect("read");
        assert_eq!(response, control_preamble());

        // A same-version client still works afterwards.
        let echoed = crate::UnixControlClient::new(&path)
            .send(envelope())
            .await
            .expect("same-version request should succeed");
        assert_eq!(echoed, envelope());

        task.abort();
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unary_client_reports_mismatch_from_skewed_server() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let path = socket_path("unary-client");
        let _ = std::fs::remove_file(&path);
        let listener = tokio::net::UnixListener::bind(&path).expect("bind");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = Vec::new();
            stream.read_to_end(&mut request).await.expect("read");
            assert_eq!(request[..CONTROL_PREAMBLE_BYTES], control_preamble());
            stream.write_all(&skewed_preamble()).await.expect("write");
            stream
                .write_all(b"layout-incompatible archive")
                .await
                .expect("write");
        });

        let err = crate::UnixControlClient::new(&path)
            .send(envelope())
            .await
            .expect_err("mismatch should surface");
        assert_eq!(err, EXPECTED_MISMATCH);
        server.await.expect("server");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn garbage_is_a_decode_failure() {
        assert!(matches!(
            check_control_preamble(b"{ no"),
            Err(IpcTransportError::DecodeFailed(_))
        ));
        assert!(matches!(
            check_control_preamble(b"OC"),
            Err(IpcTransportError::DecodeFailed(_))
        ));
    }
}
