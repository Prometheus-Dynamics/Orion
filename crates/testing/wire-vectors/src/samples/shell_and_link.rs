//! Samples of the shell's kinds and the link's.

use orion_msg_session::{
    LinkSealed, LinkSignAnswer, LinkSignChallenge, LinkSignHello, LinkSignProve, ShellClose,
    ShellCredit, ShellData, ShellEof, ShellExit, ShellOpen, ShellOpened, ShellResize,
};
use orion_wire::{Bytes, body::encode_fields};

use super::{Sample, decode_like, kind_of};

pub(super) fn samples(out: &mut Vec<Sample>) {
    sample!(
        out,
        ShellOpen {
            account: "root",
            command: Bytes(b"uname -a"),
            term: "xterm",
            cols: 80,
            rows: 24,
            window: 65536
        }
    );
    sample!(
        out,
        ShellOpened {
            request_seq: 9,
            shell: 1,
            window: 65536
        }
    );
    sample!(
        out,
        ShellData {
            shell: 1,
            stream: 1,
            data: Bytes(b"ls\n")
        }
    );
    sample!(
        out,
        ShellCredit {
            shell: 1,
            bytes: 4096
        }
    );
    sample!(
        out,
        ShellResize {
            shell: 1,
            cols: 120,
            rows: 40
        }
    );
    sample!(out, ShellEof { shell: 1 });
    sample!(out, ShellExit { shell: 1, code: 0 });
    sample!(out, ShellClose { shell: 1 });
    sample!(
        out,
        LinkSignHello {
            hello: Bytes(&[7; 96])
        }
    );
    sample!(
        out,
        LinkSignChallenge {
            hello: Bytes(&[8; 96]),
            signature: Bytes(&[9; 64])
        }
    );
    sample!(
        out,
        LinkSignProve {
            signature: Bytes(&[10; 64])
        }
    );
    sample!(
        out,
        LinkSignAnswer {
            status: 2,
            why: "the key was refused"
        }
    );
    sample!(
        out,
        LinkSealed {
            frame: Bytes(&[1, 2, 3, 4]),
            tag: Bytes(&[11; 16])
        }
    );
}
