mod support;

use support::run_orionctl;

#[test]
fn orionctl_rejects_tls_material_for_plain_http_targets() {
    let output = run_orionctl([
        "get",
        "health",
        "--http",
        "http://127.0.0.1:9100",
        "--ca-cert",
        "/tmp/unused.pem",
    ]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("plain http:// targets do not use TLS material"));
}

#[test]
fn orionctl_requires_https_ca_when_client_identity_is_provided() {
    let output = run_orionctl([
        "get",
        "health",
        "--http",
        "https://127.0.0.1:9100",
        "--client-cert",
        "/tmp/client-cert.pem",
        "--client-key",
        "/tmp/client-key.pem",
    ]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("HTTPS trust material requires --ca-cert"));
}
