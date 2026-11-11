//! The comm core's helper programs as a program of their own: what a binary that holds the
//! comm core does when it is started as a helper, and nothing else. For images that keep
//! the helpers apart from the comm core, and for trying the comm core's SSH without its
//! binary.

fn main() {
    let code = orion_comm_app::helper(std::env::args_os().skip(1)).unwrap_or_else(|| {
        eprintln!("orion-comm-helper: started by the comm core, not by hand");
        2
    });
    std::process::exit(code);
}
