use super::*;
use std::fs;

fn tree() -> PathBuf {
    let root = crate::test_temp::unique("identity", "root");
    let put = |path: &str, contents: &[u8]| {
        let full = root.join(path);
        fs::create_dir_all(full.parent().unwrap()).unwrap();
        fs::write(full, contents).unwrap();
    };
    put("proc/sys/kernel/hostname", b"board-a\n");
    put(
        "etc/os-release",
        b"ID=demo\nVERSION_ID=\"2.1\"\nNAME=Demo\n",
    );
    put("proc/device-tree/serial-number", b"0708C973B00A37BC\0");
    put("sys/class/net/eth0/address", b"2C:CF:67:00:00:01\n");
    put("sys/class/net/lo/address", b"00:00:00:00:00:00\n");
    put("sys/class/net/usb0/address", b"00:00:00:00:00:00\n");
    root
}

fn config(base: Option<&str>, security: &[&str]) -> IdentityConfig {
    IdentityConfig {
        addr: "127.0.0.1:0".parse().unwrap(),
        base: base.map(PathBuf::from),
        security_files: security.iter().map(PathBuf::from).collect(),
        ssh_host_keys: None,
    }
}

#[test]
fn without_a_package_document_the_node_builds_a_minimal_one() {
    let root = tree();
    let doc = IdentitySource::with_root(root.as_path(), &config(None, &[])).document(1_700_000_000);
    assert_eq!(doc["contract"], 1);
    assert_eq!(doc["serial"], "0708c973b00a37bc");
    assert_eq!(doc["hostname"], "board-a");
    assert_eq!(doc["os"], json!({ "name": "demo", "version": "2.1" }));
    assert_eq!(doc["macs"], json!({ "eth0": "2c:cf:67:00:00:01" }));
    assert_eq!(doc["update_methods"], json!(["image-write"]));
    assert_eq!(
        doc["device_package"],
        json!({ "version": null, "commit": null })
    );
    assert!(doc["model"].is_null() && doc["rev"].is_null() && doc["manage_url"].is_null());
    assert_eq!(doc["time"], 1_700_000_000);
    assert!(doc.get("security").is_none());
}

#[test]
fn the_package_document_passes_through_with_live_fields_on_top() {
    let root = tree();
    fs::create_dir_all(root.as_path().join("run/board")).unwrap();
    fs::write(
        root.as_path().join("run/board/identity.json"),
        br#"{"contract":1,"model":"m1","rev":"gen1","hostname":"stale","time":1,"camera_streams":[{"url":":5800/x"}]}
"#,
    )
    .unwrap();
    let source = IdentitySource::with_root(
        root.as_path(),
        &config(Some("/run/board/identity.json"), &[]),
    );
    let doc = source.document(42);
    assert_eq!(doc["model"], "m1");
    assert_eq!(doc["camera_streams"], json!([{ "url": ":5800/x" }]));
    // Live: the kernel's hostname and the request time win over the file's copies.
    assert_eq!(doc["hostname"], "board-a");
    assert_eq!(doc["time"], 42);
    // A hostname change shows on the next request, with no restart.
    fs::write(
        root.as_path().join("proc/sys/kernel/hostname"),
        b"board-b\n",
    )
    .unwrap();
    assert_eq!(source.document(43)["hostname"], "board-b");
}

#[test]
fn a_broken_package_document_falls_back_to_the_built_in_one() {
    let root = tree();
    fs::write(root.as_path().join("identity.json"), b"[1,2,3]").unwrap();
    let doc =
        IdentitySource::with_root(root.as_path(), &config(Some("/identity.json"), &[])).document(0);
    assert_eq!(doc["serial"], "0708c973b00a37bc");
}

#[test]
fn security_comes_from_the_last_file_that_sets_it() {
    let root = tree();
    fs::write(
        root.as_path().join("image.env"),
        b"# image default\nBOARD_SECURITY=0\n",
    )
    .unwrap();
    fs::write(root.as_path().join("board.env"), b"BOARD_SECURITY=\"1\"\n").unwrap();
    let both = config(None, &["/image.env", "/missing.env", "/board.env"]);
    let source = IdentitySource::with_root(root.as_path(), &both);
    assert_eq!(source.document(0)["security"], "on");
    // Live: turning the switch off shows at once.
    fs::write(root.as_path().join("board.env"), b"BOARD_SECURITY=0\n").unwrap();
    assert_eq!(source.document(0)["security"], "off");
    let image_only = IdentitySource::with_root(root.as_path(), &config(None, &["/image.env"]));
    assert_eq!(image_only.document(0)["security"], "off");
}

#[test]
fn requests_get_the_document_or_a_plain_error() {
    let root = tree();
    let source = IdentitySource::with_root(root.as_path(), &config(None, &[]));
    let ok = String::from_utf8(respond(
        b"GET /.well-known/pd-device HTTP/1.1\r\nHost: x\r\n\r\n",
        &source,
    ))
    .unwrap();
    assert!(ok.starts_with("HTTP/1.1 200 OK\r\n"), "{ok}");
    assert!(ok.contains("Content-Type: application/json\r\n"));
    let body = ok.split("\r\n\r\n").nth(1).unwrap();
    let doc: Value = serde_json::from_str(body).unwrap();
    assert_eq!(doc["serial"], "0708c973b00a37bc");
    let length: usize = ok
        .lines()
        .find_map(|line| line.strip_prefix("Content-Length: "))
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(length, body.len());

    let head = respond(b"HEAD /.well-known/pd-device HTTP/1.1\r\n\r\n", &source);
    assert!(head.ends_with(b"\r\n\r\n"));
    let missing = respond(b"GET /other HTTP/1.1\r\n\r\n", &source);
    assert!(missing.starts_with(b"HTTP/1.1 404 "));
    let post = respond(b"POST /.well-known/pd-device HTTP/1.1\r\n\r\n", &source);
    assert!(post.starts_with(b"HTTP/1.1 405 "));
    let garbage = respond(b"\xff\xfe\r\n\r\n", &source);
    assert!(garbage.starts_with(b"HTTP/1.1 405 "));
}

#[tokio::test]
async fn the_server_answers_over_tcp_and_survives_bad_clients() {
    let root = tree();
    let server = IdentityServer::start_with_source(
        "127.0.0.1:0".parse().unwrap(),
        IdentitySource::with_root(root.as_path(), &config(None, &[])),
    )
    .await
    .unwrap();
    let addr = server.local_addr();

    // A client that sends an oversized head is dropped without an answer.
    let mut flood = TcpStream::connect(addr).await.unwrap();
    let _ = flood.write_all(&[b'a'; MAX_REQUEST + 2048]).await;
    let mut sink = Vec::new();
    let _ = flood.read_to_end(&mut sink).await;
    assert!(sink.is_empty());

    // A client that connects and says nothing does not hold up the others.
    let _silent = TcpStream::connect(addr).await.unwrap();

    let mut client = TcpStream::connect(addr).await.unwrap();
    client
        .write_all(b"GET /.well-known/pd-device HTTP/1.1\r\nHost: board\r\n\r\n")
        .await
        .unwrap();
    let mut answer = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), client.read_to_end(&mut answer))
        .await
        .unwrap()
        .unwrap();
    let answer = String::from_utf8(answer).unwrap();
    assert!(answer.starts_with("HTTP/1.1 200 OK"), "{answer}");
    assert!(answer.contains("\"hostname\":\"board-a\""));
    server.shutdown().await;
}

#[test]
fn mac_addresses_are_validated() {
    assert!(is_mac("2c:cf:67:00:00:01"));
    assert!(!is_mac("2c:cf:67:00:00"));
    assert!(!is_mac("2c-cf-67-00-00-01"));
}
