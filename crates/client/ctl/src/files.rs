//! `orion file`: files to a board, and from one.

use std::io::Write;
use std::time::Duration;

use orion::Orion;
use orion::files::{FileState, OfferedBy, Pull, Step};

use crate::args::Args;
use crate::{Done, Failed, value, wait_option};

pub(crate) fn push(orion: &Orion, args: &mut Args, out: &mut dyn Write) -> Done {
    let destination = args.word("a destination")?;
    let path = args.word("a file")?;
    args.done()?;
    let mut file =
        std::fs::File::open(&path).map_err(|error| Failed::Usage(format!("{path}: {error}")))?;
    let ended = orion.files().push(&destination, &mut file, |_, _| {})?;
    finished_transfer(&destination, &ended, out)
}

fn finished_transfer(
    destination: &str,
    ended: &orion::files::Transfer,
    out: &mut dyn Write,
) -> Done {
    if ended.state == FileState::Committed {
        writeln!(out, "{destination} is in place, {} bytes", ended.received)?;
        Ok(())
    } else {
        writeln!(out, "{destination} was rejected: {}", ended.detail)?;
        Err(Failed::Unsuccessful)
    }
}

pub(crate) fn pull(orion: &Orion, args: &mut Args, out: &mut dyn Write) -> Done {
    let destination = args.word("a destination")?;
    let url = args.word("a URL")?;
    let size = args
        .number("--size")?
        .ok_or_else(|| Failed::Usage("missing --size".into()))?;
    let sum = args
        .option("--sha256")?
        .ok_or_else(|| Failed::Usage("missing --sha256".into()))?;
    let digest: [u8; 32] = value::unhex(&sum)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| Failed::Usage("--sha256 is 64 hex digits".into()))?;
    let wait = wait_option(args, 600_000)?;
    args.done()?;
    // The same file to the same place is the same transfer, so asking again follows it.
    let transfer = u64::from_le_bytes([
        digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
    ]);
    let ended = orion.files().pull(
        &Pull {
            transfer,
            url: &url,
            destination: &destination,
            size,
            digest,
        },
        wait,
        |_, _| {},
    )?;
    finished_transfer(&destination, &ended, out)
}

/// Fetches a file a program on the board offers, into a file here. What that file holds
/// already is taken to be the start of it, so a fetch that broke off goes on.
pub(crate) fn fetch(orion: &Orion, args: &mut Args, out: &mut dyn Write) -> Done {
    let name = args.word("a name")?;
    let path = args.word("a file")?;
    let bound = args.number("--bound")?;
    let give_up = args.number("--give-up")?;
    args.done()?;
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|error| Failed::Usage(format!("{path}: {error}")))?;
    let have = file
        .metadata()
        .map_err(|error| Failed::Usage(format!("{path}: {error}")))?
        .len();
    let mut fetching = orion.files().start_fetch(&name, &mut file, have)?;
    if let Some(bound) = bound {
        fetching = fetching.within(bound);
    }
    // Never given up on unless asked: a stall is said, and waited out.
    if let Some(give_up) = give_up {
        fetching = fetching.give_up_after(Duration::from_millis(give_up));
    }
    let ended = loop {
        match fetching.step(Duration::from_millis(200))? {
            Step::Done(ended) => break ended,
            Step::Stalled {
                received,
                size,
                quiet,
            } => {
                let of = size.map_or_else(String::new, |size| format!(" of {size}"));
                writeln!(
                    out,
                    "{name}: no progress for {} s, {received}{of} bytes here; still waiting",
                    quiet.as_secs()
                )?;
            }
            Step::Waiting | Step::Progress { .. } => {}
        }
    };
    if ended.state == FileState::Committed {
        writeln!(out, "{name} is in {path}, {} bytes", ended.received)?;
        Ok(())
    } else {
        writeln!(out, "{name} was not fetched: {}", ended.detail)?;
        Err(Failed::Unsuccessful)
    }
}

/// What is offered on the board, prefix by prefix.
pub(crate) fn offers(orion: &Orion, args: &mut Args, out: &mut dyn Write) -> Done {
    args.done()?;
    let (offers, clashes) = orion.files().offers()?;
    if offers.is_empty() {
        writeln!(out, "nothing is offered")?;
    }
    for offer in &offers {
        match offer.by {
            OfferedBy::Program => writeln!(
                out,
                "{}  {} ({}), since {} ms",
                offer.prefix, offer.name, offer.principal, offer.since_ms
            )?,
            OfferedBy::Configured => writeln!(out, "{}  configured", offer.prefix)?,
            _ => writeln!(out, "{}  the comm core", offer.prefix)?,
        }
    }
    if clashes > 0 {
        writeln!(
            out,
            "{clashes} offers refused: another program held the prefix"
        )?;
    }
    Ok(())
}
