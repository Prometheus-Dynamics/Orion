//! Wiring a [`StreamDevice`] to any `embedded-io` byte stream (UART, RS-485, USB-CDC).

use embedded_io::{Read, ReadReady, Write};
use orion_link::Stream;
use orion_link::device::{DeviceConfig, DeviceEvent, PublishError, StreamDevice};
use orion_link::wire::{ProviderBody, ResourceBody, StatusBody};

use crate::{RX, TX};

/// Bytes moved per driver call.
const CHUNK: usize = 32;

/// A device session bound to a UART.
///
/// ```ignore
/// let mut port = UartPort::new(uart, "imu-board");
/// port.publish(&PROVIDER, &RESOURCES)?;         // `wire` views, no heap
/// loop {
///     port.service(millis())?;               // RX → session → TX
///     while let Some(event) = port.next_event() {
///         // DeviceEvent::LeasesChanged => port.session().leases(): start/stop work
///     }
/// }
/// ```
pub struct UartPort<U> {
    uart: U,
    session: StreamDevice<RX, TX>,
}

impl<U: Read + ReadReady + Write> UartPort<U> {
    /// Binds `uart` to a new session for `device_name`.
    pub fn new(uart: U, device_name: &'static str) -> Self {
        Self::with_config(uart, DeviceConfig::provider(device_name))
    }

    /// Binds `uart` with a custom configuration (timing, roles).
    pub fn with_config(uart: U, config: DeviceConfig) -> Self {
        Self {
            uart,
            session: StreamDevice::new(config, Stream),
        }
    }

    /// One main-loop iteration: reads every byte the UART has, advances the session clock, and
    /// writes everything the session wants to send. Never blocks on reads; writes block only as
    /// long as the UART's `write` does.
    ///
    /// # Errors
    ///
    /// The UART's error; the session state is unaffected and the next call continues.
    pub fn service(&mut self, now_ms: u64) -> Result<(), U::Error> {
        let mut buf = [0u8; CHUNK];
        while self.uart.read_ready()? {
            let n = self.uart.read(&mut buf)?;
            if n == 0 {
                break;
            }
            self.session.receive(buf.get(..n).unwrap_or_default());
        }
        self.session.poll(now_ms);
        loop {
            let n = self.session.transmit(&mut buf);
            if n == 0 {
                break;
            }
            self.uart.write_all(buf.get(..n).unwrap_or_default())?;
        }
        self.uart.flush()
    }

    /// Replaces the provider snapshot (sent and retransmitted until acknowledged): `wire` views,
    /// or the full records with the `alloc` feature.
    ///
    /// # Errors
    ///
    /// [`PublishError::TooLarge`] if it does not fit [`TX`].
    pub fn publish<P, R>(&mut self, provider: &P, resources: &[R]) -> Result<(), PublishError>
    where
        P: ProviderBody + ?Sized,
        R: ResourceBody,
    {
        self.session.publish_provider_state(provider, resources)
    }

    /// Publishes volatile status values (fire-and-forget, newest batch wins).
    ///
    /// # Errors
    ///
    /// [`PublishError::TooLarge`] if the batch does not fit [`TX`].
    pub fn publish_status<S: StatusBody>(&mut self, entries: &[S]) -> Result<(), PublishError> {
        self.session.publish_status(entries)
    }

    /// The next session event.
    pub fn next_event(&mut self) -> Option<DeviceEvent> {
        self.session.next_event()
    }

    /// The session, for state queries and stats.
    pub fn session(&self) -> &StreamDevice<RX, TX> {
        &self.session
    }

    /// The UART, for example to change its baud rate.
    pub fn uart_mut(&mut self) -> &mut U {
        &mut self.uart
    }

    /// The UART back.
    pub fn into_inner(self) -> U {
        self.uart
    }
}
