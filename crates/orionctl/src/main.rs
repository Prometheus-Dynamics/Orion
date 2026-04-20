mod cli;
mod handlers;
mod render;
mod transport;

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    match handlers::run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
