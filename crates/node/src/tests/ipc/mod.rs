use super::*;

mod local_sessions;
mod stream;
mod unary;

fn build_ipc_app(socket_name: &str) -> (PathBuf, NodeApp) {
    let socket_path = temp_socket_path(socket_name);
    let app = NodeApp::builder()
        .config(test_node_config_for_socket_path(
            "node-a",
            socket_path.clone(),
            crate::PeerAuthenticationMode::Optional,
        ))
        .try_build()
        .expect("node app should build");
    (socket_path, app)
}

fn build_stream_ipc_app(control_name: &str) -> (PathBuf, NodeApp) {
    let (socket_path, app) = build_ipc_app(control_name);
    (socket_path, app)
}

fn build_local_session_app(socket_name: &str, session_ttl: Duration) -> NodeApp {
    NodeApp::builder()
        .config(test_node_config_for_socket_path(
            "node-a",
            temp_socket_path(socket_name),
            crate::PeerAuthenticationMode::Optional,
        ))
        .with_local_session_ttl(session_ttl)
        .try_build()
        .expect("node app should build")
}
