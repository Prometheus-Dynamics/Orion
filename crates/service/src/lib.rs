//! Small middleware primitives for request/response style services.
//!
//! This crate intentionally favors ergonomic composition over zero-cost static dispatch.
//! `MiddlewareStack` stores both middleware and the terminal service behind `Arc<dyn ...>`,
//! which keeps the API simple and cloneable at the cost of dynamic dispatch and heap
//! indirection per layer. That tradeoff is acceptable for Orion's control-plane boundary,
//! but this crate should not be treated as a high-performance inlined middleware pipeline.

use std::sync::Arc;

pub trait RequestService<Request>: Send + Sync + 'static {
    type Response;
    type Error;

    fn serve(&self, request: Request) -> Result<Self::Response, Self::Error>;
}

impl<Request, Response, Error, F> RequestService<Request> for F
where
    F: Fn(Request) -> Result<Response, Error> + Send + Sync + 'static,
{
    type Response = Response;
    type Error = Error;

    fn serve(&self, request: Request) -> Result<Self::Response, Self::Error> {
        (self)(request)
    }
}

pub trait RequestMiddleware<Request>: Send + Sync + 'static {
    type Response;
    type Error;

    fn handle(
        &self,
        request: Request,
        next: MiddlewareNext<'_, Request, Self::Response, Self::Error>,
    ) -> Result<Self::Response, Self::Error>;
}

pub struct MiddlewareNext<'a, Request, Response, Error> {
    middlewares: &'a [Arc<dyn RequestMiddleware<Request, Response = Response, Error = Error>>],
    terminal: &'a dyn RequestService<Request, Response = Response, Error = Error>,
}

impl<'a, Request: 'static, Response: 'static, Error: 'static>
    MiddlewareNext<'a, Request, Response, Error>
{
    fn new(
        middlewares: &'a [Arc<
            dyn RequestMiddleware<Request, Response = Response, Error = Error>,
        >],
        terminal: &'a dyn RequestService<Request, Response = Response, Error = Error>,
    ) -> Self {
        Self {
            middlewares,
            terminal,
        }
    }

    pub fn run(self, request: Request) -> Result<Response, Error> {
        match self.middlewares.split_first() {
            Some((current, remaining)) => {
                current.handle(request, Self::new(remaining, self.terminal))
            }
            None => self.terminal.serve(request),
        }
    }
}

/// Cloneable middleware pipeline backed by `Arc<dyn Trait>` objects.
///
/// This is an ergonomic composition surface, not a zero-cost abstraction. If a caller needs a
/// fully inlined hot-path pipeline, a generic service stack would be a better fit.
pub struct MiddlewareStack<Request, Response, Error> {
    middlewares: Vec<Arc<dyn RequestMiddleware<Request, Response = Response, Error = Error>>>,
    terminal: Arc<dyn RequestService<Request, Response = Response, Error = Error>>,
}

impl<Request: 'static, Response: 'static, Error: 'static> Clone
    for MiddlewareStack<Request, Response, Error>
{
    fn clone(&self) -> Self {
        Self {
            middlewares: self.middlewares.clone(),
            terminal: self.terminal.clone(),
        }
    }
}

impl<Request: 'static, Response: 'static, Error: 'static>
    MiddlewareStack<Request, Response, Error>
{
    pub fn new<S>(terminal: S) -> Self
    where
        S: RequestService<Request, Response = Response, Error = Error>,
    {
        Self {
            middlewares: Vec::new(),
            terminal: Arc::new(terminal),
        }
    }

    pub fn with_middleware<M>(mut self, middleware: M) -> Self
    where
        M: RequestMiddleware<Request, Response = Response, Error = Error>,
    {
        self.middlewares.push(Arc::new(middleware));
        self
    }

    pub fn with_arc_middleware(
        mut self,
        middleware: Arc<dyn RequestMiddleware<Request, Response = Response, Error = Error>>,
    ) -> Self {
        self.middlewares.push(middleware);
        self
    }

    pub fn serve(&self, request: Request) -> Result<Response, Error> {
        MiddlewareNext::new(&self.middlewares, self.terminal.as_ref()).run(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct RequestLogEntry(&'static str);

    struct RecordMiddleware {
        label: &'static str,
        log: Arc<Mutex<Vec<RequestLogEntry>>>,
    }

    impl RequestMiddleware<String> for RecordMiddleware {
        type Response = String;
        type Error = ();

        fn handle(
            &self,
            request: String,
            next: MiddlewareNext<'_, String, Self::Response, Self::Error>,
        ) -> Result<Self::Response, Self::Error> {
            self.log
                .lock()
                .expect("middleware log should not be poisoned")
                .push(RequestLogEntry(self.label));
            next.run(format!("{request}->{label}", label = self.label))
        }
    }

    struct ShortCircuitMiddleware;

    impl RequestMiddleware<String> for ShortCircuitMiddleware {
        type Response = String;
        type Error = ();

        fn handle(
            &self,
            request: String,
            _next: MiddlewareNext<'_, String, Self::Response, Self::Error>,
        ) -> Result<Self::Response, Self::Error> {
            Ok(format!("{request}->short"))
        }
    }

    #[test]
    fn middleware_runs_in_registration_order_before_terminal_service() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let pipeline =
            MiddlewareStack::new(|request: String| Ok::<_, ()>(format!("{request}->terminal")))
                .with_middleware(RecordMiddleware {
                    label: "outer",
                    log: log.clone(),
                })
                .with_middleware(RecordMiddleware {
                    label: "inner",
                    log: log.clone(),
                });

        let response = pipeline
            .serve("request".to_owned())
            .expect("pipeline should succeed");

        assert_eq!(response, "request->outer->inner->terminal");
        assert_eq!(
            *log.lock().expect("middleware log should not be poisoned"),
            vec![RequestLogEntry("outer"), RequestLogEntry("inner")]
        );
    }

    #[test]
    fn middleware_can_short_circuit_terminal_service() {
        let pipeline =
            MiddlewareStack::new(|request: String| Ok::<_, ()>(format!("{request}->terminal")))
                .with_middleware(ShortCircuitMiddleware);

        let response = pipeline
            .serve("request".to_owned())
            .expect("pipeline should succeed");

        assert_eq!(response, "request->short");
    }
}
