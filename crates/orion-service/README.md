# orion-service

Small request/response middleware primitives for Orion.

This crate intentionally favors ergonomic composition over fully inlined static dispatch.

Design boundary:

- `MiddlewareStack` is cloneable and simple because it stores middleware and the terminal service
  behind `Arc<dyn ...>` objects.
- That makes it a good fit for Orion's control-plane and transport boundary composition.
- It is not intended to be a zero-cost hot-path abstraction. Callers that need fully inlined
  request dispatch should use a generic stack instead of extending this crate in that direction.
