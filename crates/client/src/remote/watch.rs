//! Poll-based watches. `orion+tcp` is request/response only, so a watch re-queries the node
//! every interval (default [`super::RemoteOperatorConfig::poll_interval`], 500 ms) and yields
//! only when something changed. Each poll is one signed request and response.

use super::{RemoteError, RemoteOperator};
use orion_control_plane::{ActionQuery, ActionResult, StatusEntry, StatusQuery};
use std::{collections::BTreeMap, time::Duration};

impl RemoteOperator {
    /// Watches the node's status lane: [`RemoteStatusWatch::next`] returns the full matching set
    /// whenever it differs from the previous one (an entry was added, changed or expired).
    pub fn watch_status(&self, query: StatusQuery) -> RemoteStatusWatch {
        RemoteStatusWatch {
            operator: self.clone(),
            query,
            interval: self.config().poll_interval,
            last: None,
        }
    }

    /// Watches tracked actions: [`RemoteActionWatch::next`] returns the results that are new or
    /// changed since the previous call (the first call returns every matching result).
    pub fn watch_actions(&self, query: ActionQuery) -> RemoteActionWatch {
        RemoteActionWatch {
            operator: self.clone(),
            query,
            interval: self.config().poll_interval,
            seen: BTreeMap::new(),
            primed: false,
        }
    }
}

/// See [`RemoteOperator::watch_status`].
#[derive(Debug)]
pub struct RemoteStatusWatch {
    operator: RemoteOperator,
    query: StatusQuery,
    interval: Duration,
    last: Option<Vec<StatusEntry>>,
}

impl RemoteStatusWatch {
    /// Changes the poll interval (at least 50 ms).
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval.max(Duration::from_millis(50));
        self
    }

    /// Waits for the next change and returns the current matching entries. The first call
    /// returns immediately.
    pub async fn next(&mut self) -> Result<Vec<StatusEntry>, RemoteError> {
        loop {
            if self.last.is_some() {
                tokio::time::sleep(self.interval).await;
            }
            let mut entries = self.operator.status(self.query.clone()).await?;
            entries.sort_by(|a, b| (&a.subject, &a.key).cmp(&(&b.subject, &b.key)));
            if self.last.as_ref() != Some(&entries) {
                self.last = Some(entries.clone());
                return Ok(entries);
            }
        }
    }
}

/// See [`RemoteOperator::watch_actions`].
#[derive(Debug)]
pub struct RemoteActionWatch {
    operator: RemoteOperator,
    query: ActionQuery,
    interval: Duration,
    seen: BTreeMap<String, ActionResult>,
    primed: bool,
}

impl RemoteActionWatch {
    /// Changes the poll interval (at least 50 ms).
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval.max(Duration::from_millis(50));
        self
    }

    /// Waits until at least one matching result is new or changed and returns those results.
    pub async fn next(&mut self) -> Result<Vec<ActionResult>, RemoteError> {
        loop {
            if self.primed {
                tokio::time::sleep(self.interval).await;
            }
            self.primed = true;
            let results = self.operator.query_actions(self.query.clone()).await?;
            let changed: Vec<ActionResult> = results
                .into_iter()
                .filter(|result| self.seen.get(&result.action_id) != Some(result))
                .collect();
            if changed.is_empty() {
                continue;
            }
            for result in &changed {
                self.seen.insert(result.action_id.clone(), result.clone());
            }
            return Ok(changed);
        }
    }
}
