//! A remote operator (`docs/remote-operator.md`): enrolls with a node, lists the cluster's nodes
//! with their host facts, runs an action and waits for its result.
//!
//! ```sh
//! # On the node: ORION_NODE_PEER_ADDR=0.0.0.0:9200 ORION_NODE_PEER_AUTH=required
//! # (plus ORION_NODE_DISCOVERY=mdns ORION_NODE_ENROLLMENT_KEY=... for shared-key enrollment).
//! ORION_NODE_URL=orion+tcp://10.0.0.2:9200 \
//! ORION_OPERATOR_KEY_FILE=$HOME/.config/my-tool/operator.key \
//! ORION_ENROLLMENT_KEY=... \
//! ORION_ACTION=locate \
//!   cargo run -p orion-client --no-default-features --features remote --example remote_operator
//! ```
//!
//! Without `ORION_ENROLLMENT_KEY` the example prints the operator's fingerprint for an
//! administrator to approve (`orionctl operators enroll operator:<name> --fingerprint ...`).

use orion_client::remote::{
    ActionRequest, ActionTarget, NodeTrust, OperatorIdentity, RemoteError, RemoteOperator,
};
use std::{path::PathBuf, time::Duration};

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

/// Loads the operator key from `path`, or creates and stores one. The caller owns storage; a
/// real tool would use the OS keyring or a file only the user can read.
fn load_identity(name: &str, path: &PathBuf) -> Result<OperatorIdentity, RemoteError> {
    if let Ok(secret) = std::fs::read(path) {
        return OperatorIdentity::from_secret_key_bytes(name, &secret);
    }
    let identity = OperatorIdentity::generate(name)?;
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    std::fs::write(path, identity.secret_key_bytes())
        .map_err(|err| RemoteError::InvalidIdentity(format!("{}: {err}", path.display())))?;
    Ok(identity)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), RemoteError> {
    let url = env_or("ORION_NODE_URL", "orion+tcp://127.0.0.1:9200");
    let name = env_or("ORION_OPERATOR_NAME", "example");
    let key_file = PathBuf::from(env_or(
        "ORION_OPERATOR_KEY_FILE",
        &std::env::temp_dir()
            .join("orion-remote-operator.key")
            .display()
            .to_string(),
    ));
    let identity = load_identity(&name, &key_file)?;
    println!(
        "operator {} fingerprint {}",
        identity.operator_id(),
        identity.fingerprint()
    );

    // First use: pin whatever key the node presents. Store `operator.node_public_key()` and use
    // `NodeTrust::Node { .. }` afterwards; shared-key enrollment also authenticates the node.
    let operator = RemoteOperator::connect(&url, identity, NodeTrust::FirstUse).await?;
    println!(
        "connected to {} (key {})",
        operator.node_id(),
        operator.node_fingerprint()
    );
    if !operator.is_enrolled() {
        match std::env::var("ORION_ENROLLMENT_KEY") {
            Ok(key) => {
                operator.enroll_with_key(key.trim().as_bytes()).await?;
                println!("enrolled with the shared enrollment key");
            }
            Err(_) => {
                println!(
                    "not enrolled; on {} run: orionctl operators enroll {} --fingerprint {}",
                    operator.node_id(),
                    operator.identity().operator_id(),
                    operator.identity().fingerprint()
                );
                return Ok(());
            }
        }
    }
    let welcome = operator.welcome();
    println!(
        "policy read={} actions={:?}",
        welcome.read, welcome.allowed_actions
    );

    for node in operator.nodes().await? {
        let host = node.host.unwrap_or_default();
        println!(
            "node {} health={:?} hostname={} os={} {} image={} {} boot_id={}",
            node.node_id,
            node.health,
            host.hostname.as_deref().unwrap_or("-"),
            host.os_id.as_deref().unwrap_or("-"),
            host.os_version.as_deref().unwrap_or("-"),
            host.image_name.as_deref().unwrap_or("-"),
            host.image_version.as_deref().unwrap_or("-"),
            host.boot_id.as_deref().unwrap_or("-"),
        );
    }

    let action = env_or("ORION_ACTION", "locate");
    let target = std::env::var("ORION_ACTION_NODE")
        .map(orion_core::NodeId::new)
        .unwrap_or_else(|_| operator.node_id().clone());
    let action_id = format!(
        "example-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis())
            .unwrap_or_default()
    );
    let accepted = operator
        .run_action(ActionRequest::new(
            action_id.clone(),
            ActionTarget::Node(target),
            action,
        ))
        .await?;
    println!(
        "action {} {} on {}",
        accepted.action_id, accepted.state, accepted.handled_by
    );
    let done = operator
        .wait_for_action(&action_id, Duration::from_secs(60))
        .await?;
    println!(
        "action {} finished: {} output={:?}",
        done.action_id, done.state, done.output
    );
    Ok(())
}
