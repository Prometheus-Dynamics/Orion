//! `orion operator`: asking a board to enroll a key as an operator.

use std::io::Write;

use orion::Orion;
use orion::node::{Enrollment, OperatorState};

use crate::args::{Args, Usage};
use crate::{Done, Failed};

/// Which operator verb, checked before anything is asked of a board.
pub(crate) enum Verb {
    /// This operator asks for its own key (`--key`).
    Ask([u8; 32]),
    /// Someone on the board enrolls any key.
    Enroll,
}

/// The verb, if it is one and has what it needs.
pub(crate) fn understood(verb: &str, own: Option<[u8; 32]>) -> Result<Verb, Usage> {
    match (verb, own) {
        ("ask", Some(key)) => Ok(Verb::Ask(key)),
        ("ask", None) => Err(Usage(
            "`operator ask` asks for the key in --key FILE: name one".to_owned(),
        )),
        ("enroll", _) => Ok(Verb::Enroll),
        _ => Err(Usage(format!("there is no `operator {verb}`"))),
    }
}

fn key_from_hex(text: &str) -> Option<[u8; 32]> {
    let text = text.trim();
    if text.len() != 64 || !text.is_ascii() {
        return None;
    }
    let mut key = [0_u8; 32];
    for (byte, pair) in key.iter_mut().zip(text.as_bytes().chunks(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(key)
}

pub(crate) fn run(orion: &Orion, verb: Verb, args: &mut Args, out: &mut dyn Write) -> Done {
    let (key, enrollment_key) = match verb {
        Verb::Ask(own) => {
            // From a file, so the secret is not on the command line for others to see.
            let secret = match args.option("--enrollment-key-file")? {
                Some(path) => Some(
                    std::fs::read_to_string(path)?
                        .trim_end()
                        .as_bytes()
                        .to_vec(),
                ),
                None => None,
            };
            (own, secret)
        }
        Verb::Enroll => {
            let text = args.word("the key to enroll, in hex")?;
            let key = key_from_hex(&text)
                .ok_or_else(|| Usage(format!("{text} is not a key: 64 hex digits")))?;
            (key, None)
        }
    };
    let name = args
        .option("--as")?
        .ok_or_else(|| Usage("an operator needs --as NAME, a name for people".to_owned()))?;
    args.done()?;
    let answer = orion.enroll(key, &name, enrollment_key.as_deref())?;
    show(&answer, &key, out)?;
    if answer.state == OperatorState::Enrolled {
        Ok(())
    } else {
        Err(Failed::Unsuccessful)
    }
}

fn hex(key: &[u8; 32]) -> String {
    key.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn show(answer: &Enrollment, key: &[u8; 32], out: &mut dyn Write) -> std::io::Result<()> {
    let state = match answer.state {
        OperatorState::Enrolled => "enrolled",
        OperatorState::Pending => "pending: someone on the board approves it",
        OperatorState::Revoked => "revoked",
        _ => "unknown",
    };
    writeln!(out, "operator    {state}")?;
    writeln!(out, "its key     {}", hex(key))?;
    writeln!(out, "node        {}", answer.node)?;
    writeln!(
        out,
        "node key    {} ({})",
        hex(&answer.node_key),
        answer.fingerprint
    )?;
    writeln!(out, "cluster     {}", answer.cluster)?;
    if answer.state == OperatorState::Enrolled {
        let actions = if answer.all_actions {
            "all".to_owned()
        } else if answer.actions.is_empty() {
            "none".to_owned()
        } else {
            answer.actions.join(", ")
        };
        writeln!(
            out,
            "may read    {}",
            if answer.read { "yes" } else { "no" }
        )?;
        writeln!(out, "may run     {actions}")?;
    }
    Ok(())
}
