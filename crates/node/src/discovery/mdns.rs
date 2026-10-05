//! Real mDNS/DNS-SD backend on top of the `mdns-sd` crate (pure Rust, its own daemon thread,
//! no async runtime). See `docs/discovery.md` for why this crate was chosen.

use super::{
    advert::SERVICE_TYPE,
    backend::{Announcement, DiscoveryBackend, DiscoveryEvent, DiscoveryEventSender},
};
use mdns_sd::{IfKind, ServiceDaemon, ServiceEvent, ServiceInfo};
use std::{
    net::IpAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tracing::{debug, warn};

/// How often the browse is restarted so live instances are re-reported from the daemon cache
/// (which keeps their entries in the discovered set from expiring).
const DEFAULT_REFRESH: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(250);

/// mDNS backend: advertises `_orion._tcp.local.` and browses for it.
pub struct MdnsDiscoveryBackend {
    interfaces: Vec<String>,
    refresh: Duration,
    daemon: Option<ServiceDaemon>,
    fullname: Option<String>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl MdnsDiscoveryBackend {
    /// `interfaces` restricts mDNS to these interface names or addresses (empty: all).
    pub fn new(interfaces: Vec<String>) -> Self {
        Self {
            interfaces,
            refresh: DEFAULT_REFRESH,
            daemon: None,
            fullname: None,
            stop: Arc::new(AtomicBool::new(false)),
            thread: None,
        }
    }

    /// Re-report period for live instances; keep it well below the discovery TTL.
    pub fn with_refresh(mut self, refresh: Duration) -> Self {
        self.refresh = refresh.max(Duration::from_secs(1));
        self
    }

    fn restrict_interfaces(&self, daemon: &ServiceDaemon) -> Result<(), String> {
        if self.interfaces.is_empty() {
            return Ok(());
        }
        daemon
            .disable_interface(IfKind::All)
            .map_err(|err| err.to_string())?;
        let kinds: Vec<IfKind> = self
            .interfaces
            .iter()
            .map(|name| match name.parse::<IpAddr>() {
                Ok(addr) => IfKind::Addr(addr),
                Err(_) => IfKind::Name(name.clone()),
            })
            .collect();
        daemon
            .enable_interface(kinds)
            .map_err(|err| err.to_string())
    }
}

/// mDNS host label for an instance: lowercase `[a-z0-9-]`, at most 63 bytes.
fn host_label(instance: &str) -> String {
    let mut label: String = instance
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    label.truncate(55);
    format!("orion-{}", label.trim_matches('-'))
}

impl DiscoveryBackend for MdnsDiscoveryBackend {
    fn name(&self) -> &'static str {
        "mdns"
    }

    fn start(&mut self, local: &Announcement, events: DiscoveryEventSender) -> Result<(), String> {
        let daemon = ServiceDaemon::new().map_err(|err| err.to_string())?;
        self.restrict_interfaces(&daemon)?;
        let host = format!("{}.local.", host_label(&local.instance));
        let txt: Vec<(&str, &str)> = local
            .txt
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        let info = ServiceInfo::new(
            SERVICE_TYPE,
            &local.instance,
            &host,
            local.addresses.as_slice(),
            local.port,
            txt.as_slice(),
        )
        .map_err(|err| err.to_string())?;
        let info = if local.addresses.is_empty() {
            info.enable_addr_auto()
        } else {
            info
        };
        let fullname = info.get_fullname().to_owned();
        daemon.register(info).map_err(|err| err.to_string())?;
        let mut receiver = daemon.browse(SERVICE_TYPE).map_err(|err| err.to_string())?;
        let stop = self.stop.clone();
        let refresh = self.refresh;
        let browse_daemon = daemon.clone();
        let own = fullname.clone();
        let thread = std::thread::Builder::new()
            .name("orion-mdns".into())
            .spawn(move || {
                let mut last_browse = Instant::now();
                while !stop.load(Ordering::Relaxed) {
                    match receiver.recv_timeout(POLL) {
                        Ok(ServiceEvent::ServiceResolved(service)) if service.fullname != own => {
                            let announcement = Announcement {
                                instance: service.fullname.clone(),
                                addresses: service
                                    .addresses
                                    .iter()
                                    .map(|ip| ip.to_ip_addr())
                                    .collect(),
                                port: service.port,
                                txt: service
                                    .txt_properties
                                    .iter()
                                    .map(|prop| (prop.key().to_owned(), prop.val_str().to_owned()))
                                    .collect(),
                                ttl: None,
                            };
                            let _ = events.try_send(DiscoveryEvent::Announced(announcement));
                        }
                        Ok(ServiceEvent::ServiceRemoved(_, fullname)) if fullname != own => {
                            let _ =
                                events.try_send(DiscoveryEvent::Withdrawn { instance: fullname });
                        }
                        Ok(other) => debug!(event = ?other, "mdns event"),
                        Err(mdns_sd::RecvTimeoutError::Timeout) => {}
                        Err(mdns_sd::RecvTimeoutError::Disconnected) => break,
                    }
                    if last_browse.elapsed() >= refresh {
                        // Restarting the browse re-reports cached instances and re-queries.
                        match browse_daemon.browse(SERVICE_TYPE) {
                            Ok(next) => receiver = next,
                            Err(err) => warn!(error = %err, "failed to refresh the mDNS browse"),
                        }
                        last_browse = Instant::now();
                    }
                }
            })
            .map_err(|err| err.to_string())?;
        self.daemon = Some(daemon);
        self.fullname = Some(fullname);
        self.thread = Some(thread);
        Ok(())
    }

    fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(daemon) = self.daemon.take() {
            if let Some(fullname) = self.fullname.take()
                && let Ok(status) = daemon.unregister(&fullname)
            {
                // Wait briefly for the goodbye packets to go out.
                let _ = status.recv_timeout(Duration::from_secs(1));
            }
            if let Ok(status) = daemon.shutdown() {
                let _ = status.recv_timeout(Duration::from_secs(1));
            }
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for MdnsDiscoveryBackend {
    fn drop(&mut self) {
        self.stop();
    }
}
