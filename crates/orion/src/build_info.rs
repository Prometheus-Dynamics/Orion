/// Orion package semver from Cargo metadata.
pub const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Orion build commit resolved at compile time.
pub const BUILD_COMMIT: &str = env!("ORION_BUILD_COMMIT");

/// Human-readable Orion build version.
pub const BUILD_VERSION: &str = env!("ORION_BUILD_VERSION");

/// Returns the Cargo package version for this build.
pub const fn pkg_version() -> &'static str {
    PKG_VERSION
}

/// Returns the git commit identifier captured for this build.
pub const fn build_commit() -> &'static str {
    BUILD_COMMIT
}

/// Returns the semver plus commit identifier for this build.
pub const fn build_version() -> &'static str {
    BUILD_VERSION
}

#[cfg(test)]
mod tests {
    use super::{build_commit, build_version, pkg_version};

    #[test]
    fn build_info_reports_semver_and_commit() {
        assert_eq!(pkg_version(), env!("CARGO_PKG_VERSION"));
        assert!(!build_commit().is_empty());
        assert!(build_version().contains(pkg_version()));
        assert!(build_version().contains(build_commit()));
    }
}
