//! Every carrier this build can serve, as one type, and the tasks that serve them.

use orion_unix::UnixCarrier;
use tokio::task::JoinHandle;

/// Tasks that end when this is dropped: a comm core that stops, or one whose start failed
/// part way, leaves nothing serving behind it.
#[derive(Default)]
pub(crate) struct Tasks(Vec<JoinHandle<()>>);

impl std::ops::Deref for Tasks {
    type Target = Vec<JoinHandle<()>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Tasks {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for Tasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

/// Every carrier this build can serve, as one type the server is generic over.
macro_rules! carriers {
    ($( $(#[$cfg:meta])* $variant:ident($carrier:ty, $connection:ty) ),* $(,)?) => {
        // One of each per carrier or connection, made once: their size is what they hold.
        #[allow(clippy::large_enum_variant)]
        pub(crate) enum Carriers { $( $(#[$cfg])* $variant($carrier), )* }
        #[allow(clippy::large_enum_variant)]
        pub(crate) enum Connections { $( $(#[$cfg])* $variant($connection), )* }
        #[allow(clippy::large_enum_variant)]
        pub(crate) enum Reads { $( $(#[$cfg])* $variant(<$connection as orion_server::Connection>::Read), )* }
        #[allow(clippy::large_enum_variant)]
        pub(crate) enum Writes { $( $(#[$cfg])* $variant(<$connection as orion_server::Connection>::Write), )* }

        impl orion_server::Carrier for Carriers {
            type Connection = Connections;

            async fn accept(&mut self) -> std::io::Result<Connections> {
                match self {
                    $( $(#[$cfg])* Self::$variant(carrier) => carrier.accept().await.map(Connections::$variant), )*
                }
            }
        }

        impl orion_server::Connection for Connections {
            type Read = Reads;
            type Write = Writes;

            fn principal(&self) -> orion_server::Principal {
                match self { $( $(#[$cfg])* Self::$variant(connection) => connection.principal(), )* }
            }

            fn offers(&self) -> orion_server::Roles {
                match self { $( $(#[$cfg])* Self::$variant(connection) => connection.offers(), )* }
            }

            fn passes_descriptors(&self) -> bool {
                match self { $( $(#[$cfg])* Self::$variant(connection) => connection.passes_descriptors(), )* }
            }

            fn split(self) -> (Reads, Writes) {
                match self {
                    $( $(#[$cfg])* Self::$variant(connection) => {
                        let (read, write) = connection.split();
                        (Reads::$variant(read), Writes::$variant(write))
                    } )*
                }
            }
        }

        impl orion_server::FrameRead for Reads {
            async fn recv(&mut self, buf: &mut [u8], fds: &mut orion_server::FdSet) -> std::io::Result<Option<usize>> {
                match self { $( $(#[$cfg])* Self::$variant(read) => read.recv(buf, fds).await, )* }
            }
        }

        impl orion_server::FrameWrite for Writes {
            async fn send(&mut self, frame: &[u8], fds: &[std::os::fd::OwnedFd]) -> std::io::Result<()> {
                match self { $( $(#[$cfg])* Self::$variant(write) => write.send(frame, fds).await, )* }
            }
        }
    };
}

carriers! {
    Unix(UnixCarrier, orion_unix::UnixConnection),
    #[cfg(feature = "tcp")]
    Tcp(orion_tcp::TcpCarrier, orion_tcp::TcpConnection),
    #[cfg(feature = "ws")]
    Ws(orion_ws::WsCarrier, orion_ws::WsConnection),
    #[cfg(feature = "link-serial")]
    Serial(
        orion_link_serial::StreamCarrier<orion_link_serial::Serial>,
        orion_link_serial::StreamConnection<orion_link_serial::SerialPort>
    ),
    #[cfg(feature = "link-sealed")]
    Sealed(
        orion_link_serial::StreamCarrier<orion_link_serial::sealed::SealedListener>,
        orion_link_serial::StreamConnection<orion_link_serial::sealed::SealedStream>
    ),
    #[cfg(feature = "link-usb")]
    Usb(
        orion_link_serial::StreamCarrier<orion_link_usb::UsbLink>,
        orion_link_serial::StreamConnection<orion_link_usb::UsbStream>
    ),
    #[cfg(feature = "link-can")]
    Can(
        orion_link_can::CanCarrier<orion_link_can::SocketCanBus>,
        orion_link_can::CanConnection<orion_link_can::SocketCan>
    ),
}
