//! The session helper, for this crate's tests: what a shell is started in. The comm core's
//! own binary is its helper on a board. See `orion_ssh::helper`.

fn main() {
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref() != Some(orion_ssh::helper::MODE.as_ref()) {
        eprintln!("orion-shell-helper: started by the shell service, not by hand");
        std::process::exit(2);
    }
    std::process::exit(orion_ssh::helper::main(args));
}
