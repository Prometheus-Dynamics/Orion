//! What the browser tests share: a browser to run pages in, a site to serve them from, and a
//! publisher of the video example's test clip.
//!
//! The tests run `web/client` and `web/video-example` in a real browser against a comm core:
//! see `tests/`.

pub mod browser;
pub mod video;
