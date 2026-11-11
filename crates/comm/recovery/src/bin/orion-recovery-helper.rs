//! The session helper as a program of its own, for trying the recovery way in without the
//! comm core's binary. See `orion_ssh::helper`.

fn main() {
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref() != Some(orion_ssh::helper::MODE.as_ref()) {
        eprintln!("orion-recovery-helper: started by the SSH server, not by hand");
        std::process::exit(2);
    }
    std::process::exit(orion_ssh::helper::main(args));
}
