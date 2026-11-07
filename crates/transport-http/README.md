# orion-transport-http

HTTP transport adapter for Orion control-plane traffic, including optional TLS and mTLS support.

Features:

- none: protocol layer only (payloads, routes, codec, errors, handler traits).
- `client`: `HttpClient` and `HttpClientTlsConfig` (HTTP/1.1 over tokio, rustls for https; no
  reqwest). Does not link the axum/hyper server stack.
- `server`: `HttpServer` and the server TLS / client-auth configuration (axum, hyper,
  tokio-rustls).
- `transport` (default): `client` + `server`.
