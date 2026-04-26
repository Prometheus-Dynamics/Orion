use std::{env, process::Command};

pub fn emit_build_info() {
    println!("cargo:rerun-if-env-changed=ORION_BUILD_COMMIT");
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/index");

    let pkg_version = env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".to_owned());
    let commit = env::var("ORION_BUILD_COMMIT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(resolve_git_commit);
    let version = format!("{pkg_version} ({commit})");

    println!("cargo:rustc-env=ORION_BUILD_VERSION={version}");
    println!("cargo:rustc-env=ORION_BUILD_COMMIT={commit}");
}

fn resolve_git_commit() -> String {
    Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .map(|stdout| stdout.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}
