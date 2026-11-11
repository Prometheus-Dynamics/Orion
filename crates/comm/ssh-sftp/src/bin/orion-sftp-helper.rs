//! Both helpers as one program, for trying the sftp subsystem without the comm core's
//! binary: the session helper of `orion-ssh` and the sftp helper of this crate.

fn main() {
    let mut args = std::env::args_os().skip(1);
    let mode = args.next();
    let code = match mode.as_ref().and_then(|mode| mode.to_str()) {
        Some(orion_ssh::helper::MODE) => orion_ssh::helper::main(args),
        Some(orion_ssh_sftp::MODE) => orion_ssh_sftp::main(),
        _ => {
            eprintln!("orion-sftp-helper: started by the SSH server, not by hand");
            2
        }
    };
    std::process::exit(code);
}
