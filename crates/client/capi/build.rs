//! Gives the shared library a name with its major version in it, so the loader refuses a
//! library of another major before any call is made.

fn main() {
    println!("cargo::rerun-if-changed=build.rs");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux") {
        println!("cargo::rustc-cdylib-link-arg=-Wl,-soname,liborion_c.so.1");
    }
}
