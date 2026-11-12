//! The trust store on its own: identity, operators and their grants, peers, revocation, and
//! what is on disk.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use orion_keys::enroll::{operator_proof, proof};
use orion_keys::{Identity, Keys, KeysConfig, KeysError, OperatorState, fingerprint, verify};
use orion_server::{Principal, Security, What};
use orion_types::{KeyId, NodeName};

fn directory(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("orion-keys-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    path
}

fn config(dir: &Path) -> KeysConfig {
    KeysConfig {
        state_dir: Some(dir.to_owned()),
        cluster: "lab".to_owned(),
        enrollment_key: Some(b"correct horse".to_vec()),
        operators_read: true,
        operators_actions: vec!["locate".to_owned(), "light.*".to_owned()],
    }
}

fn open(dir: &Path) -> Keys {
    Keys::open(config(dir), Security::On).expect("keys")
}

fn node() -> NodeName {
    "board".parse().expect("name")
}

#[test]
fn the_nodes_identity_is_made_once_kept_from_others_and_never_replaced() {
    let dir = directory("identity");
    let first = open(&dir).identity().public();
    let file = dir.join("node-identity.ed25519");
    let mode = |file: &Path| {
        std::fs::metadata(file)
            .expect("key file")
            .permissions()
            .mode()
            & 0o777
    };
    // Its owner and its group, node logic's, may read it; nobody else.
    assert_eq!(mode(&file), 0o640);
    // The same board the next time. A key an older build made owner-only is opened to the
    // group.
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    assert_eq!(open(&dir).identity().public(), first);
    assert_eq!(mode(&file), 0o640);
    assert!(fingerprint(&first).starts_with("sha256:") && fingerprint(&first).len() == 39);

    // A key file that is there but is not a key stops the node. It is not written over.
    std::fs::write(&file, b"damaged").expect("write");
    assert!(matches!(
        Keys::open(config(&dir), Security::On),
        Err(KeysError::Identity(_))
    ));
    assert_eq!(std::fs::read(&file).expect("still there"), b"damaged");

    // Signatures made with an identity verify against its public key and nothing else.
    let identity = Identity::generate();
    let signature = identity.sign(b"message");
    assert!(verify(&identity.public(), b"message", &signature));
    assert!(!verify(&identity.public(), b"another", &signature));
    assert!(!verify(
        &Identity::generate().public(),
        b"message",
        &signature
    ));
    assert!(!verify(&identity.public(), b"message", &signature[..63]));
    assert_eq!(
        Identity::from_secret(&identity.secret()).public(),
        identity.public()
    );
}

#[test]
fn an_operator_is_enrolled_by_an_administrator_by_proof_or_waits() {
    let dir = directory("operators");
    let mut keys = open(&dir);
    let (asker, prover, approved) = ([1; 32], [2; 32], [3; 32]);

    // Asking with nothing: pending, and nothing granted.
    let operator = keys
        .operator_asks(&node(), asker, "alice laptop", b"")
        .expect("ask");
    assert_eq!(
        (operator.state, operator.name.as_str()),
        (OperatorState::Pending, "alice_laptop")
    );
    assert!(
        !keys
            .operator(&KeyId(asker))
            .expect("recorded")
            .may(What::Read("x"))
    );

    // Asking with proof of the enrollment key: enrolled, with the default grants.
    let good = proof(b"correct horse", &operator_proof("board", &prover));
    let operator = keys
        .operator_asks(&node(), prover, "bob", &good)
        .expect("ask");
    assert_eq!(operator.state, OperatorState::Enrolled);
    // A proof for another key, another node or with another secret is no proof.
    for bad in [
        proof(b"correct horse", &operator_proof("board", &asker)),
        proof(b"correct horse", &operator_proof("other", &[9; 32])),
        proof(b"wrong", &operator_proof("board", &[9; 32])),
    ] {
        let operator = keys
            .operator_asks(&node(), [9; 32], "eve", &bad)
            .expect("ask");
        assert_eq!(operator.state, OperatorState::Pending);
    }

    // An administrator approves the one that was waiting.
    assert_eq!(
        keys.approve_operator(asker, "alice")
            .expect("approve")
            .state,
        OperatorState::Enrolled
    );
    keys.approve_operator(approved, "carol").expect("approve");

    // What an enrolled operator may do: read, and the actions it was given.
    let alice = keys.operator(&KeyId(asker)).expect("alice").clone();
    assert!(alice.may(What::Read("status")) && alice.may(What::Open("cpu.temp")));
    assert!(alice.may(What::Run("locate")) && alice.may(What::Run("light.set")));
    assert!(!alice.may(What::Run("reboot")) && !alice.may(What::Run("locator")));
    assert!(!alice.may(What::Write("workload")) && !alice.may(What::Login));
    // Grants can be changed, up to everything.
    assert!(
        keys.grant(&KeyId(asker), false, vec!["login".to_owned()], false)
            .expect("grant")
    );
    let alice = keys.operator(&KeyId(asker)).expect("alice").clone();
    assert!(
        !alice.may(What::Read("status"))
            && alice.may(What::Login)
            && !alice.may(What::Run("locate"))
    );
    assert!(
        keys.grant(&KeyId(asker), true, Vec::new(), true)
            .expect("grant")
    );
    let alice = keys.operator(&KeyId(asker)).expect("alice").clone();
    assert!(
        alice.may(What::Run("anything"))
            && alice.may(What::Write("workload"))
            && alice.may(What::Login)
    );
    assert!(
        !keys
            .grant(&KeyId([77; 32]), true, Vec::new(), true)
            .expect("nobody")
    );

    // All of it is still there after a restart.
    let keys = open(&dir);
    assert_eq!(keys.operators().count(), 4);
    assert!(
        keys.operator(&KeyId(asker))
            .expect("alice")
            .may(What::Write("workload"))
    );
    assert_eq!(
        keys.operator(&KeyId(prover)).expect("bob").actions,
        ["locate", "light.*"]
    );
    assert_eq!(
        keys.operator(&KeyId([9; 32])).expect("eve").state,
        OperatorState::Pending
    );
}

#[test]
fn a_refused_operator_is_not_enrolled_again_by_asking_or_by_proof() {
    let dir = directory("refused");
    let mut keys = open(&dir);
    let (pending, enrolled) = ([1; 32], [2; 32]);
    let proof_for = |key: &[u8; 32]| proof(b"correct horse", &operator_proof("board", key));

    // One that was waiting and is turned down; one that was enrolled and is revoked.
    keys.operator_asks(&node(), pending, "mallory", b"")
        .expect("ask");
    keys.revoke(pending).expect("refuse");
    keys.operator_asks(&node(), enrolled, "trent", &proof_for(&enrolled))
        .expect("ask");
    assert!(
        keys.operator(&KeyId(enrolled))
            .expect("trent")
            .may(What::Read("x"))
    );
    keys.revoke(enrolled).expect("revoke");

    for key in [pending, enrolled] {
        assert!(keys.is_revoked(&key));
        assert!(
            !keys
                .operator(&KeyId(key))
                .expect("recorded")
                .may(What::Read("x"))
        );
        // Asking again, even with a good proof, changes nothing.
        let again = keys
            .operator_asks(&node(), key, "again", &proof_for(&key))
            .expect("ask");
        assert_eq!(again.state, OperatorState::Revoked);
        assert_eq!(
            keys.approve_if_new(key, "again").expect("ask").state,
            OperatorState::Revoked
        );
        assert_eq!(keys.principal(&key), Principal::Anonymous);
    }
    // Not after a restart either.
    let mut keys = open(&dir);
    assert_eq!(
        keys.operator_asks(&node(), enrolled, "x", &proof_for(&enrolled))
            .expect("ask")
            .state,
        OperatorState::Revoked
    );
    // Only an administrator brings one back, with what it had.
    assert_eq!(
        keys.approve_operator(enrolled, "trent")
            .expect("approve")
            .state,
        OperatorState::Enrolled
    );
    assert!(!keys.is_revoked(&enrolled));
    assert!(
        keys.operator(&KeyId(enrolled))
            .expect("trent")
            .may(What::Run("locate"))
    );
}

#[test]
fn a_peer_is_pinned_to_one_key() {
    let dir = directory("peers");
    let mut keys = open(&dir);
    let left: NodeName = "left".parse().expect("name");
    assert!(keys.trust_peer(&left, [5; 32]).expect("trust"));
    assert_eq!(keys.peer_key(&left), Some(&[5; 32]));
    // The same key again is fine. Another key for the same name is not.
    assert!(keys.trust_peer(&left, [5; 32]).expect("trust"));
    assert!(!keys.trust_peer(&left, [6; 32]).expect("another key"));
    assert_eq!(keys.peer_key(&left), Some(&[5; 32]));

    // A connection is known by its key: a pinned peer, else an operator.
    assert_eq!(keys.principal(&[5; 32]), Principal::Peer(left.clone()));
    assert_eq!(
        keys.principal(&[8; 32]),
        Principal::Operator(KeyId([8; 32]))
    );

    // Revoking the key forgets the peer, and the key cannot be pinned again.
    keys.revoke([5; 32]).expect("revoke");
    assert_eq!(keys.peer_key(&left), None);
    assert!(!keys.trust_peer(&left, [5; 32]).expect("revoked"));
    let keys = open(&dir);
    assert_eq!((keys.peers().count(), keys.is_revoked(&[5; 32])), (0, true));
}

#[test]
fn lines_a_newer_build_wrote_are_kept() {
    let dir = directory("unknown");
    std::fs::create_dir_all(&dir).expect("dir");
    let key = "ab".repeat(32);
    std::fs::write(
        dir.join("trust"),
        format!("# a comment\npeer left {key}\ncertificate left some-new-thing 42\noperator not-a-key\n\n"),
    )
    .expect("write");
    let mut keys = open(&dir);
    assert_eq!(keys.peers().count(), 1);
    keys.trust_peer(&"right".parse().expect("name"), [1; 32])
        .expect("trust");
    let text = std::fs::read_to_string(dir.join("trust")).expect("read");
    assert!(
        text.contains("certificate left some-new-thing 42"),
        "{text}"
    );
    assert!(text.contains("operator not-a-key"), "{text}");
    assert!(text.contains(&format!("peer left {key}")) && text.contains("peer right "));
    // The store on disk is private to the node.
    assert!(!dir.join("trust.tmp").exists());
}

#[test]
fn with_no_state_directory_nothing_is_kept() {
    let mut keys = Keys::open(KeysConfig::in_memory("lab"), Security::On).expect("keys");
    keys.approve_operator([1; 32], "alice").expect("approve");
    assert_eq!((keys.cluster(), keys.enrollment_key()), ("lab", None));
    // No enrollment key: a proof of anything is no proof.
    let asked = keys
        .operator_asks(&node(), [2; 32], "bob", &[0; 32])
        .expect("ask");
    assert_eq!(asked.state, OperatorState::Pending);
}
