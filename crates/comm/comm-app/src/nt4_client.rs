//! The NT4 gateway as a client of a server somewhere else (`nt4.server`), and that server's
//! clock told to the time sync, which says it in `orion.time`.

use std::cell::RefCell;
use std::rc::Rc;

use orion_broker::Topics;
use orion_nt4_client_gateway::{ClientGateway, FarClock, Nt4ClientConfig};

use crate::error::{StartError, failed};

/// Where the server's clock is told: the time sync's, when there is one.
#[cfg(feature = "time")]
pub(crate) type Told = orion_time::FarClock;

/// No time sync built: nowhere.
#[cfg(not(feature = "time"))]
#[derive(Debug)]
pub(crate) enum Told {}

/// Starts the gateway, telling `far` the server's clock each time it is measured anew, and
/// that it is gone when the server is lost.
pub(crate) fn start(
    topics: &Rc<RefCell<Topics>>,
    config: Nt4ClientConfig,
    far: Option<Told>,
) -> Result<ClientGateway, StartError> {
    let server = format!("nt4.server {}:{}", config.host, config.port);
    let told = move |clock: Option<FarClock>| {
        #[cfg(feature = "time")]
        if let Some(far) = &far {
            far.set(clock.map(|clock| (clock.offset_us, clock.rtt_us)));
        }
        let _ = (&far, clock);
    };
    ClientGateway::start_telling(topics, config, told).map_err(|error| failed(server, error))
}
