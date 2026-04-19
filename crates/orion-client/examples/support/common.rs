use std::{env, future::Future};

pub(crate) type ExampleError = Box<dyn std::error::Error>;

pub(crate) async fn exit_on_error(future: impl Future<Output = Result<(), ExampleError>>) {
    if let Err(error) = future.await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

pub(crate) fn read_exact_args<const N: usize>() -> Result<[String; N], ExampleError> {
    let values = env::args().skip(1).collect::<Vec<_>>();
    if values.len() != N {
        return Err(format!("expected {N} args, got {}", values.len()).into());
    }
    Ok(values
        .try_into()
        .map_err(|_| "argument count conversion failed")?)
}
