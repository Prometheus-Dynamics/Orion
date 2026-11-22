//! A refusal from node logic, told as the session it is for hears it.

use orion_msg_session::{NodeAnswer, Refusal};
use orion_wire::{Decode, kind};

/// Node logic's refusal as a session at `level` is to be told it, if that is not as it came.
pub(crate) fn told<'a>(answer: &NodeAnswer<'a>, level: u16) -> Option<Refusal<'a>> {
    if answer.kind != kind::REFUSAL {
        return None;
    }
    let refusal = Refusal::decode(answer.payload.0).ok()?;
    let error = refusal.error()?;
    let told = orion_server::spoken(error, level);
    (told != error).then(|| Refusal {
        request_seq: refusal.request_seq,
        code: told.code(),
        detail: told.as_str(),
    })
}

#[cfg(test)]
mod tests {
    use orion_wire::body::encode_fields;

    use orion_server::Error;
    use orion_wire::Bytes;

    use super::*;

    fn refusal(error: Error, buf: &mut [u8]) -> usize {
        let refusal = Refusal {
            request_seq: 7,
            code: error.code(),
            detail: error.as_str(),
        };
        encode_fields(&refusal, buf).expect("encode")
    }

    #[test]
    fn a_refusal_from_node_logic_is_told_as_the_sessions_level_hears_it() {
        let mut buf = [0_u8; 128];
        let len = refusal(Error::Restoring, &mut buf);
        let answer = |kind| NodeAnswer {
            session: 1,
            kind,
            may_drop: false,
            payload: Bytes(&buf[..len]),
        };
        // Below the level: not found, under the same request.
        let older = told(&answer(kind::REFUSAL), orion_server::RESTORING_LEVEL - 1).expect("told");
        assert_eq!(
            (older.request_seq, older.error(), older.detail),
            (7, Some(Error::NotFound), Error::NotFound.as_str())
        );
        // At the level, and for anything that is not a refusal: as it came.
        assert!(told(&answer(kind::REFUSAL), orion_server::RESTORING_LEVEL).is_none());
        assert!(told(&answer(kind::PONG), 0).is_none());
        let len = refusal(Error::Busy, &mut buf);
        let busy = NodeAnswer {
            session: 1,
            kind: kind::REFUSAL,
            may_drop: false,
            payload: Bytes(&buf[..len]),
        };
        assert!(told(&busy, 0).is_none());
    }
}
