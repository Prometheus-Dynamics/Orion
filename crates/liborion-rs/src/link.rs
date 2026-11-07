//! A client session on a node's `unix:` link.

use std::os::fd::RawFd;
use std::path::Path;
use std::time::Duration;

use crate::event::Event;
use crate::sys::{self, Handle, OrionEvent, Status};
use crate::{Error, c_option, c_string, check, check_version, error, ptr, timeout_ms};

/// What a request is about (`ORION_SUBJECT_*`): a status subject, an action's target, a record
/// kind, a stream's owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Subject {
    Provider,
    Executor,
    Resource,
    Workload,
    Node,
}

impl Subject {
    /// `ORION_SUBJECT_*`.
    pub fn code(self) -> u32 {
        match self {
            Self::Provider => sys::ORION_SUBJECT_PROVIDER,
            Self::Executor => sys::ORION_SUBJECT_EXECUTOR,
            Self::Resource => sys::ORION_SUBJECT_RESOURCE,
            Self::Workload => sys::ORION_SUBJECT_WORKLOAD,
            Self::Node => sys::ORION_SUBJECT_NODE,
        }
    }
}

/// A link session. Calls take `&mut self`; the session may move between threads. Closed on drop.
#[derive(Debug)]
pub struct Link {
    handle: Handle,
    buf: Vec<u8>,
}

fn request(status: Status, req: u32) -> Result<u32, Error> {
    check(status).map(|()| req)
}

impl Link {
    /// Connects to the link socket at `path` as `name` and waits up to `timeout` for the node to
    /// accept the session. Checks the library's version first.
    pub fn connect(path: &Path, name: &str, timeout: Duration) -> Result<Self, Error> {
        check_version()?;
        let path = c_string(&path.to_string_lossy(), "path")?;
        let name = c_string(name, "name")?;
        let ms = u32::try_from(timeout.as_millis()).unwrap_or(u32::MAX);
        let mut handle = Handle::NULL;
        // SAFETY: valid strings and handle.
        check(unsafe { sys::orion_link_connect(path.as_ptr(), name.as_ptr(), ms, &mut handle) })?;
        Ok(Self {
            handle,
            buf: vec![0; 2048],
        })
    }

    /// The session's socket, to poll for reading; call [`Self::next`] when it is readable or
    /// [`Self::service_in`] has passed.
    pub fn fd(&self) -> Result<RawFd, Error> {
        let mut fd = -1;
        // SAFETY: a writable int.
        check(unsafe { sys::orion_link_fd(self.handle, &mut fd) })?;
        Ok(fd)
    }

    /// How long until the session needs a [`Self::next`] call at the latest.
    pub fn service_in(&self) -> Result<Duration, Error> {
        let mut ms = 0;
        // SAFETY: a writable u32.
        check(unsafe { sys::orion_link_service_in_ms(self.handle, &mut ms) })?;
        Ok(Duration::from_millis(u64::from(ms)))
    }

    fn status_request(
        &mut self,
        subject: Option<(Subject, &str)>,
        key_prefix: Option<&str>,
        watch: bool,
    ) -> Result<u32, Error> {
        let (kind, id) = subject.map_or((sys::ORION_SUBJECT_ANY, None), |(kind, id)| {
            (kind.code(), Some(id))
        });
        let id = c_option(id, "subject id")?;
        let prefix = c_option(key_prefix, "key prefix")?;
        let call = if watch {
            sys::orion_link_status_watch
        } else {
            sys::orion_link_status_query
        };
        let mut req = 0;
        // SAFETY: null or valid strings, a writable request id.
        let status = unsafe { call(self.handle, kind, ptr(&id), ptr(&prefix), &mut req) };
        request(status, req)
    }

    /// Asks once for the status entries of `subject` (all subjects for `None`) with keys starting
    /// with `key_prefix`. Returns the request id the answer carries.
    pub fn status_query(
        &mut self,
        subject: Option<(Subject, &str)>,
        key_prefix: Option<&str>,
    ) -> Result<u32, Error> {
        self.status_request(subject, key_prefix, false)
    }

    /// Watches the entries selected as for [`Self::status_query`]. One watch per session.
    pub fn status_watch(
        &mut self,
        subject: Option<(Subject, &str)>,
        key_prefix: Option<&str>,
    ) -> Result<u32, Error> {
        self.status_request(subject, key_prefix, true)
    }

    /// Runs the action `name` on `target` as `action_id`, with `args_json` (a JSON object of
    /// scalars) and a deadline (`None`: the node's). Its states follow as action events.
    pub fn action_run(
        &mut self,
        target: (Subject, &str),
        action_id: &str,
        name: &str,
        args_json: Option<&str>,
        deadline: Option<Duration>,
    ) -> Result<u32, Error> {
        let target_id = c_string(target.1, "target id")?;
        let action_id = c_string(action_id, "action id")?;
        let name = c_string(name, "name")?;
        let args = c_option(args_json, "args")?;
        let deadline_ms = deadline.map_or(0, |deadline| {
            u64::try_from(deadline.as_millis()).unwrap_or(u64::MAX)
        });
        let mut req = 0;
        // SAFETY: valid strings (args null or valid), a writable request id.
        let status = unsafe {
            sys::orion_link_action_run(
                self.handle,
                target.0.code(),
                target_id.as_ptr(),
                action_id.as_ptr(),
                name.as_ptr(),
                ptr(&args),
                deadline_ms,
                &mut req,
            )
        };
        request(status, req)
    }

    /// Watches the action `action_id` (every action for `None`).
    pub fn action_watch(&mut self, action_id: Option<&str>) -> Result<u32, Error> {
        let id = c_option(action_id, "action id")?;
        let mut req = 0;
        // SAFETY: a null or valid string, a writable request id.
        let status = unsafe { sys::orion_link_action_watch(self.handle, ptr(&id), &mut req) };
        request(status, req)
    }

    fn record_request(
        &mut self,
        kind: Subject,
        id: Option<&str>,
        node: Option<&str>,
        watch: bool,
    ) -> Result<u32, Error> {
        let id = c_option(id, "record id")?;
        let node = c_option(node, "node id")?;
        let call = if watch {
            sys::orion_link_record_watch
        } else {
            sys::orion_link_record_query
        };
        let mut req = 0;
        // SAFETY: null or valid strings, a writable request id.
        let status = unsafe { call(self.handle, kind.code(), ptr(&id), ptr(&node), &mut req) };
        request(status, req)
    }

    /// Asks once for the records of `kind` (workloads, resources, providers, executors), only
    /// `id` and only those of `node` when given.
    pub fn record_query(
        &mut self,
        kind: Subject,
        id: Option<&str>,
        node: Option<&str>,
    ) -> Result<u32, Error> {
        self.record_request(kind, id, node, false)
    }

    /// Watches the records selected as for [`Self::record_query`].
    pub fn record_watch(
        &mut self,
        kind: Subject,
        id: Option<&str>,
        node: Option<&str>,
    ) -> Result<u32, Error> {
        self.record_request(kind, id, node, true)
    }

    /// Subscribes to the stream `stream_id` of `owner`: the series named in `series` (names or
    /// `prefix*`; all when empty), at most one sample per series per `min_interval`, starting
    /// with the last `backfill` of history.
    pub fn stream_subscribe(
        &mut self,
        owner: (Subject, &str),
        stream_id: &str,
        series: &[&str],
        min_interval: Duration,
        backfill: Duration,
    ) -> Result<u32, Error> {
        let owner_id = c_string(owner.1, "owner id")?;
        let stream_id = c_string(stream_id, "stream id")?;
        if series.iter().any(|name| name.contains(',')) {
            return Err(Error {
                status: Status::INVALID_ARGUMENT,
                message: "a series name contains a comma".into(),
            });
        }
        let series = c_string(&series.join(","), "series")?;
        let mut req = 0;
        // SAFETY: valid strings, a writable request id.
        let status = unsafe {
            sys::orion_link_stream_subscribe(
                self.handle,
                owner.0.code(),
                owner_id.as_ptr(),
                stream_id.as_ptr(),
                series.as_ptr(),
                u32::try_from(min_interval.as_micros()).unwrap_or(u32::MAX),
                u64::try_from(backfill.as_millis()).unwrap_or(u64::MAX),
                &mut req,
            )
        };
        request(status, req)
    }

    /// Ends the watch or subscription `req`.
    pub fn end(&mut self, req: u32) -> Result<(), Error> {
        // SAFETY: plain values.
        check(unsafe { sys::orion_link_end(self.handle, req) })
    }

    /// The next event, waiting up to `timeout` (`None`: as long as it takes; zero: only what
    /// has arrived). `None` when nothing came. Pings the node while it waits. Any error ends the
    /// session.
    pub fn next(&mut self, timeout: Option<Duration>) -> Result<Option<Event>, Error> {
        let mut event = OrionEvent {
            struct_size: size_of::<OrionEvent>() as u32,
            ..OrionEvent::default()
        };
        let mut wait = timeout_ms(timeout);
        loop {
            // SAFETY: a valid event and a buffer of `buf.len()` bytes.
            let status = unsafe {
                sys::orion_link_next(
                    self.handle,
                    wait,
                    &mut event,
                    self.buf.as_mut_ptr(),
                    self.buf.len(),
                )
            };
            match status {
                Status::OK => {
                    let len = (event.subject_len + event.key_len + event.data_len) as usize;
                    return Ok(Some(Event::new(event, &self.buf[..len])));
                }
                Status::AGAIN => return Ok(None),
                Status::BUFFER_TOO_SMALL => {
                    let len = (event.subject_len + event.key_len + event.data_len) as usize;
                    self.buf.resize(len.max(self.buf.len() * 2), 0);
                    // The event is waiting; take it without waiting again.
                    wait = 0;
                }
                other => return Err(error(other)),
            }
        }
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        // SAFETY: this handle, closed once.
        let _ = unsafe { sys::orion_close(self.handle) };
    }
}
