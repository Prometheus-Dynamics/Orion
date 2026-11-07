//! `shared`: link liborion.so (`-lorion`), from `LIBORION_LIB_DIR` when set. `static` needs nothing:
//! the core crate defines the C functions.

fn main() {
    println!("cargo::rerun-if-env-changed=LIBORION_LIB_DIR");
    if std::env::var_os("CARGO_FEATURE_SHARED").is_some()
        && std::env::var_os("CARGO_FEATURE_STATIC").is_none()
    {
        if let Some(dir) = std::env::var_os("LIBORION_LIB_DIR") {
            println!("cargo::rustc-link-search=native={}", dir.to_string_lossy());
        }
        println!("cargo::rustc-link-lib=dylib=orion");
    }
}
