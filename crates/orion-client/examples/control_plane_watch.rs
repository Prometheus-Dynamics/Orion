use orion_client::ControlPlaneEventStream;
use orion_control_plane::ClientEventKind;
use orion_core::Revision;

#[path = "support/common.rs"]
mod common;

#[tokio::main]
async fn main() {
    common::exit_on_error(run()).await;
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let [socket_path, client_name] = common::read_exact_args::<2>()?;
    let mut stream = ControlPlaneEventStream::connect_at_with_local_address(
        &socket_path,
        client_name.clone(),
        format!("{client_name}.control"),
    )
    .await?;
    stream.subscribe_state(Revision::ZERO).await?;
    let events = stream.next_events().await?;
    let Some(event) = events.first() else {
        return Err("no control-plane events received".into());
    };
    match &event.event {
        ClientEventKind::StateSnapshot(snapshot) => {
            println!(
                "control-plane state seq={} desired_rev={} workloads={} providers={} executors={} leases={}",
                event.sequence,
                snapshot.state.desired.revision,
                snapshot.state.desired.workloads.len(),
                snapshot.state.desired.providers.len(),
                snapshot.state.desired.executors.len(),
                snapshot.state.desired.leases.len()
            );
            Ok(())
        }
        other => Err(format!("unexpected control-plane event: {other:?}").into()),
    }
}
