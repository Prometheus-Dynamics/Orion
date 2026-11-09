use std::any::Any;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::task::{Context, Poll};

/// A component panicked.
pub struct Panicked(pub Box<dyn Any + Send>);

impl Panicked {
    /// The panic message, when it was text.
    #[must_use]
    pub fn message(&self) -> &str {
        self.0
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| self.0.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("panic")
    }
}

impl std::fmt::Debug for Panicked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Panicked({:?})", self.message())
    }
}

impl std::fmt::Display for Panicked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "component panicked: {}", self.message())
    }
}

impl std::error::Error for Panicked {}

/// Runs `f`, turning a panic into an error, so one component can't take the process down.
///
/// # Errors
///
/// [`Panicked`].
pub fn contain<R>(f: impl FnOnce() -> R) -> Result<R, Panicked> {
    catch_unwind(AssertUnwindSafe(f)).map_err(Panicked)
}

/// A component task whose panics become an error, on any async runtime.
#[derive(Debug)]
pub struct Contained<F>(F);

impl<F> Contained<F> {
    /// Contains `future`.
    pub const fn new(future: F) -> Self {
        Self(future)
    }
}

impl<F: Future + Unpin> Future for Contained<F> {
    type Output = Result<F::Output, Panicked>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let future = Pin::new(&mut self.0);
        match catch_unwind(AssertUnwindSafe(|| future.poll(context))) {
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => Poll::Ready(Err(Panicked(payload))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::Waker;

    fn poll_once<F: Future + Unpin>(
        future: &mut Contained<F>,
    ) -> Poll<Result<F::Output, Panicked>> {
        Pin::new(future).poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn a_panic_becomes_an_error() {
        assert_eq!(contain(|| 5).unwrap(), 5);
        let err = contain(|| panic!("boom {}", 1)).unwrap_err();
        assert_eq!(err.message(), "boom 1");
    }

    #[test]
    fn a_task_panic_becomes_an_error() {
        let mut ok = Contained::new(Box::pin(async { 7 }));
        assert!(matches!(poll_once(&mut ok), Poll::Ready(Ok(7))));
        let mut bad = Contained::new(Box::pin(async {
            if true {
                panic!("task boom");
            }
        }));
        let Poll::Ready(Err(err)) = poll_once(&mut bad) else {
            panic!("expected the panic to be contained");
        };
        assert_eq!(err.message(), "task boom");
    }
}
