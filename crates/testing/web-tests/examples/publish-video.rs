//! Publishes the video example's test clip on a board, for `web/video-example`:
//!
//! ```text
//! publish-video <orion socket> <media directory> [samples a second]
//! ```

use std::path::Path;
use std::sync::atomic::AtomicBool;

use orion::{Address, Orion};
use orion_web_tests::video::{Clip, publish};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let (Some(socket), Some(media)) = (args.next(), args.next()) else {
        return Err(
            "usage: publish-video <orion socket> <media directory> [samples a second]".into(),
        );
    };
    let rate = args.next().map_or(Ok(10), |rate| rate.parse())?;
    let clip = Clip::load(Path::new(&media))?;
    let orion = Orion::connect(&Address::Local(socket.into()), "video-publisher")?;
    publish(&orion, &clip, rate, &AtomicBool::new(false))?;
    Ok(())
}
