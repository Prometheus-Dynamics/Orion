use orion_control_plane::{PeerEnrollment, PeerIdentityUpdate};
use orion_core::NodeId;

use crate::{
    cli::{OutputFormat, PeerCommand},
    render::{print_peer_summary, print_structured},
};

pub(super) async fn run(command: PeerCommand) -> Result<(), String> {
    match command {
        PeerCommand::List(args) => {
            let snapshot = args
                .local
                .client()?
                .query_peer_trust()
                .await
                .map_err(|error| error.to_string())?;
            match args.local.output {
                OutputFormat::Summary => {
                    print_peer_summary(&snapshot);
                    Ok(())
                }
                OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => {
                    print_structured(&snapshot, args.local.output)
                }
            }
        }
        PeerCommand::Enroll(args) => {
            let client = args.local.client()?;
            client
                .enroll_peer(PeerEnrollment {
                    node_id: NodeId::new(args.node_id),
                    base_url: args.base_url.into(),
                    trusted_public_key_hex: args.public_key.map(Into::into),
                    trusted_tls_root_cert_pem: args
                        .tls_root_cert
                        .as_deref()
                        .map(std::fs::read)
                        .transpose()
                        .map_err(|error| error.to_string())?,
                })
                .await
                .map_err(|error| error.to_string())?;
            println!("peers enroll accepted");
            Ok(())
        }
        PeerCommand::Revoke(args) => {
            args.local
                .client()?
                .revoke_peer(NodeId::new(args.node_id))
                .await
                .map_err(|error| error.to_string())?;
            println!("peers revoke accepted");
            Ok(())
        }
        PeerCommand::ReplaceKey(args) => {
            args.local
                .client()?
                .replace_peer_identity(PeerIdentityUpdate {
                    node_id: NodeId::new(args.node_id),
                    public_key_hex: args.public_key.into(),
                })
                .await
                .map_err(|error| error.to_string())?;
            println!("peers replace-key accepted");
            Ok(())
        }
        PeerCommand::RotateHttpTls(args) => {
            args.client()?
                .rotate_http_tls_identity()
                .await
                .map_err(|error| error.to_string())?;
            println!("peers rotate-http-tls accepted");
            Ok(())
        }
    }
}
