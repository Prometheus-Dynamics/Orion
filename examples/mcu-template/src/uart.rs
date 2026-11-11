//! A device on any `embedded-io` byte stream: a UART, RS-485, USB serial.

use embedded_io::{Read, ReadReady, Write};
use orion_device_core::{DeviceConfig, Event, Link, StreamDevice};

use crate::{App, RX, TX};

/// Bytes moved per driver call.
const CHUNK: usize = 32;

/// A device session on a UART.
///
/// ```ignore
/// let mut port = UartPort::new(uart, "imu-board");
/// let mut app = Imu::new();
/// loop {
///     port.service(millis(), &mut app)?;
///     while let Some(event) = port.next_event() { /* Connected, Disconnected, Rejected */ }
/// }
/// ```
pub struct UartPort<U> {
    uart: U,
    device: StreamDevice<RX, TX>,
}

impl<U: Read + ReadReady + Write> UartPort<U> {
    /// A session for the device `name` on `uart`.
    pub const fn new(uart: U, name: &'static str) -> Self {
        Self::with_config(uart, DeviceConfig::new(name))
    }

    /// A session with other timing.
    pub const fn with_config(uart: U, config: DeviceConfig) -> Self {
        Self {
            uart,
            device: StreamDevice::new(config),
        }
    }

    /// One pass of the main loop: reads what the UART has, hands frames to `app`, moves the
    /// session and `app` on to `now_ms`, and writes what is to be sent. Never waits for a
    /// byte; a write waits as long as the UART's does.
    ///
    /// # Errors
    ///
    /// The UART's. The session is untouched and the next call goes on.
    pub fn service(&mut self, now_ms: u32, app: &mut impl App<TX>) -> Result<(), U::Error> {
        let mut buf = [0_u8; CHUNK];
        while self.uart.read_ready()? {
            let n = self.uart.read(&mut buf)?;
            if n == 0 {
                break;
            }
            self.device
                .receive(buf.get(..n).unwrap_or_default(), |link, frame| {
                    app.handle(link, frame);
                });
        }
        self.device.link.poll(now_ms);
        app.poll(&mut self.device.link, now_ms);
        loop {
            let n = self.device.transmit(&mut buf);
            if n == 0 {
                break;
            }
            self.uart.write_all(buf.get(..n).unwrap_or_default())?;
        }
        self.uart.flush()
    }

    /// The next thing the firmware should know of.
    pub fn next_event(&mut self) -> Option<Event> {
        self.device.link.next_event()
    }

    /// The session, to send on outside [`UartPort::service`].
    pub fn link(&mut self) -> &mut Link<TX> {
        &mut self.device.link
    }

    /// The UART, to change its baud rate.
    pub fn uart_mut(&mut self) -> &mut U {
        &mut self.uart
    }

    /// The UART back.
    pub fn into_inner(self) -> U {
        self.uart
    }
}
