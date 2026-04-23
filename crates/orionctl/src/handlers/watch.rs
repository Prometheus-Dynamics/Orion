use orion_client::ControlPlaneEventStream;
use orion_core::Revision;

use crate::{
    cli::{OutputFormat, WatchCommand},
    render::{print_event_summary, print_structured},
};

pub(super) async fn run(command: WatchCommand) -> Result<(), String> {
    match command {
        WatchCommand::State(args) => {
            let mut stream = ControlPlaneEventStream::connect_at_with_local_address(
                &args.stream_socket,
                args.client_name.clone(),
                format!("{}.control", args.client_name),
            )
            .await
            .map_err(|error| error.to_string())?;
            stream
                .subscribe_state(Revision::new(args.desired_revision))
                .await
                .map_err(|error| error.to_string())?;
            for _ in 0..args.batches {
                let events = stream
                    .next_events()
                    .await
                    .map_err(|error| error.to_string())?;
                for event in events {
                    match args.output {
                        OutputFormat::Summary => print_event_summary(&event),
                        OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => {
                            print_structured(&event, args.output)?
                        }
                    }
                }
            }
            Ok(())
        }
    }
}
