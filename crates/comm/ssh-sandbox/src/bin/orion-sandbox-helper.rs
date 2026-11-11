//! Both programs the sandboxed SSH server starts, as one, for trying it without the comm
//! core's binary: the child that serves a connection and the helper a session runs in.

fn main() {
    let mut args = std::env::args_os().skip(1);
    let mode = args.next();
    let code = match mode.as_ref().and_then(|mode| mode.to_str()) {
        Some(orion_ssh_sandbox::child::MODE) => orion_ssh_sandbox::child::main(),
        Some(orion_ssh::helper::MODE) => orion_ssh::helper::main(args),
        _ => {
            eprintln!("orion-sandbox-helper: started by the SSH server, not by hand");
            2
        }
    };
    std::process::exit(code);
}
