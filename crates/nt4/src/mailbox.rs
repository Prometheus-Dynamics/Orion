//! A bounded, coalescing queue between one producer side and one consumer task.
//!
//! Items carry an optional key. A keyed item (a value update) replaces the pending item with the
//! same key in place, so a consumer that falls behind keeps only the latest value per key and the
//! queue never holds more than one pending update per key. Unkeyed items (control messages) are
//! never replaced or dropped; their count is capped, and a push past the cap fails with
//! [`PushError::Full`] so the caller decides what an overflowing consumer means.
//!
//! Keys are only coalesced while still queued. Once the consumer takes an item it is delivered
//! as before, so a fast consumer sees every value in order.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::Notify;

/// Unkeyed (control) items that may be queued before a push fails. A consumer that far behind is
/// not keeping up with the topic table at all.
pub(crate) const CONTROL_CAPACITY: usize = 65_536;

/// Why a push failed. The item is returned to the caller.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PushError<T> {
    /// The receiver is gone or the channel was aborted.
    Closed(T),
    /// Too many unkeyed items are queued (see [`CONTROL_CAPACITY`]).
    Full(T),
}

struct Entry<K, T> {
    seq: u64,
    key: Option<K>,
    item: T,
}

struct Inner<K, T> {
    queue: VecDeque<Entry<K, T>>,
    /// Key -> sequence number of its pending entry.
    pending: HashMap<K, u64>,
    /// Queued unkeyed entries.
    control: usize,
    next_seq: u64,
    senders: usize,
    receiver_gone: bool,
    aborted: bool,
}

struct Shared<K, T> {
    inner: Mutex<Inner<K, T>>,
    ready: Notify,
    control_cap: usize,
}

impl<K, T> Shared<K, T> {
    fn lock(&self) -> MutexGuard<'_, Inner<K, T>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Creates a mailbox. `control_cap` bounds the unkeyed items that may be queued.
pub(crate) fn channel<K, T>(control_cap: usize) -> (Sender<K, T>, Receiver<K, T>)
where
    K: Eq + Hash + Clone,
{
    let shared = Arc::new(Shared {
        inner: Mutex::new(Inner {
            queue: VecDeque::new(),
            pending: HashMap::new(),
            control: 0,
            next_seq: 0,
            senders: 1,
            receiver_gone: false,
            aborted: false,
        }),
        ready: Notify::new(),
        control_cap,
    });
    (
        Sender {
            shared: Arc::clone(&shared),
        },
        Receiver { shared },
    )
}

/// The producer side. Cloning adds a producer; the receiver ends once the last one is dropped and
/// the queue is empty.
pub(crate) struct Sender<K, T>
where
    K: Eq + Hash + Clone,
{
    shared: Arc<Shared<K, T>>,
}

impl<K, T> Sender<K, T>
where
    K: Eq + Hash + Clone,
{
    /// Queues an item. A keyed item replaces the pending item with the same key, keeping its
    /// place in the queue.
    pub(crate) fn push(&self, item: T, key: Option<K>) -> Result<(), PushError<T>> {
        let mut inner = self.shared.lock();
        if inner.receiver_gone || inner.aborted {
            return Err(PushError::Closed(item));
        }
        if let Some(k) = &key
            && let Some(&seq) = inner.pending.get(k)
            && let Ok(index) = inner.queue.binary_search_by_key(&seq, |e| e.seq)
        {
            inner.queue[index].item = item;
            return Ok(());
        }
        if key.is_none() {
            if inner.control >= self.shared.control_cap {
                return Err(PushError::Full(item));
            }
            inner.control += 1;
        }
        let seq = inner.next_seq;
        inner.next_seq += 1;
        if let Some(k) = &key {
            inner.pending.insert(k.clone(), seq);
        }
        inner.queue.push_back(Entry { seq, key, item });
        drop(inner);
        self.shared.ready.notify_one();
        Ok(())
    }

    /// Removes the pending keyed item for `key`, if any (for example a value for a topic that was
    /// just unannounced, so the consumer never sees a value before its announce or after its
    /// removal).
    pub(crate) fn purge(&self, key: &K) {
        let mut inner = self.shared.lock();
        if let Some(seq) = inner.pending.remove(key)
            && let Ok(index) = inner.queue.binary_search_by_key(&seq, |e| e.seq)
        {
            inner.queue.remove(index);
        }
    }

    /// Drops everything queued and makes every later push and receive end at once. Used when the
    /// consumer is too far behind to be worth serving.
    pub(crate) fn abort(&self) {
        let mut inner = self.shared.lock();
        inner.aborted = true;
        inner.queue.clear();
        inner.pending.clear();
        inner.control = 0;
        drop(inner);
        self.shared.ready.notify_one();
    }
}

impl<K, T> Clone for Sender<K, T>
where
    K: Eq + Hash + Clone,
{
    fn clone(&self) -> Self {
        self.shared.lock().senders += 1;
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<K, T> Drop for Sender<K, T>
where
    K: Eq + Hash + Clone,
{
    fn drop(&mut self) {
        let mut inner = self.shared.lock();
        inner.senders -= 1;
        let last = inner.senders == 0;
        drop(inner);
        if last {
            self.shared.ready.notify_one();
        }
    }
}

impl<K, T> std::fmt::Debug for Sender<K, T>
where
    K: Eq + Hash + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

/// The consumer side. Dropping it makes every later push fail with [`PushError::Closed`].
pub(crate) struct Receiver<K, T>
where
    K: Eq + Hash + Clone,
{
    shared: Arc<Shared<K, T>>,
}

impl<K, T> Receiver<K, T>
where
    K: Eq + Hash + Clone,
{
    /// The next item, or `None` once every sender is gone and the queue is empty, or the channel
    /// was aborted. Cancel-safe.
    pub(crate) async fn recv(&mut self) -> Option<T> {
        loop {
            {
                let mut inner = self.shared.lock();
                if inner.aborted {
                    return None;
                }
                if let Some(entry) = inner.queue.pop_front() {
                    match &entry.key {
                        Some(k) => {
                            if inner.pending.get(k) == Some(&entry.seq) {
                                inner.pending.remove(k);
                            }
                        }
                        None => inner.control -= 1,
                    }
                    return Some(entry.item);
                }
                if inner.senders == 0 {
                    return None;
                }
            }
            self.shared.ready.notified().await;
        }
    }
}

impl<K, T> Drop for Receiver<K, T>
where
    K: Eq + Hash + Clone,
{
    fn drop(&mut self) {
        let mut inner = self.shared.lock();
        inner.receiver_gone = true;
        inner.queue.clear();
        inner.pending.clear();
        inner.control = 0;
    }
}

impl<K, T> std::fmt::Debug for Receiver<K, T>
where
    K: Eq + Hash + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn keyed_items_coalesce_and_keep_their_place() {
        let (tx, mut rx) = channel::<u32, &str>(8);
        tx.push("a1", Some(1)).unwrap();
        tx.push("b1", Some(2)).unwrap();
        tx.push("c", None).unwrap();
        tx.push("a2", Some(1)).unwrap();
        tx.push("a3", Some(1)).unwrap();
        assert_eq!(rx.recv().await, Some("a3"));
        assert_eq!(rx.recv().await, Some("b1"));
        assert_eq!(rx.recv().await, Some("c"));
        // Taken items are not coalesced into again.
        tx.push("a4", Some(1)).unwrap();
        assert_eq!(rx.recv().await, Some("a4"));
    }

    #[tokio::test]
    async fn control_items_are_never_replaced_and_are_capped() {
        let (tx, mut rx) = channel::<u32, u32>(2);
        tx.push(1, None).unwrap();
        tx.push(2, None).unwrap();
        assert_eq!(tx.push(3, None), Err(PushError::Full(3)));
        // Keyed items do not count against the control cap.
        tx.push(9, Some(7)).unwrap();
        assert_eq!(rx.recv().await, Some(1));
        tx.push(4, None).unwrap();
        assert_eq!(rx.recv().await, Some(2));
        assert_eq!(rx.recv().await, Some(9));
        assert_eq!(rx.recv().await, Some(4));
    }

    #[tokio::test]
    async fn purge_removes_only_the_pending_keyed_item() {
        let (tx, mut rx) = channel::<u32, u32>(8);
        tx.push(1, Some(1)).unwrap();
        tx.push(2, Some(2)).unwrap();
        tx.purge(&1);
        tx.push(3, Some(1)).unwrap();
        assert_eq!(rx.recv().await, Some(2));
        assert_eq!(rx.recv().await, Some(3));
    }

    #[tokio::test]
    async fn receiver_ends_after_senders_drop_and_pushes_fail_after_receiver_drops() {
        let (tx, mut rx) = channel::<u32, u32>(8);
        tx.push(1, None).unwrap();
        let tx2 = tx.clone();
        drop(tx);
        tx2.push(2, None).unwrap();
        drop(tx2);
        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, Some(2));
        assert_eq!(rx.recv().await, None);

        let (tx, rx) = channel::<u32, u32>(8);
        drop(rx);
        assert_eq!(tx.push(1, Some(1)), Err(PushError::Closed(1)));
    }

    #[tokio::test]
    async fn abort_drops_the_queue_and_ends_the_receiver() {
        let (tx, mut rx) = channel::<u32, u32>(8);
        tx.push(1, None).unwrap();
        tx.abort();
        assert_eq!(rx.recv().await, None);
        assert_eq!(tx.push(2, None), Err(PushError::Closed(2)));
    }

    #[tokio::test]
    async fn waiting_receiver_wakes_on_push() {
        let (tx, mut rx) = channel::<u32, u32>(8);
        let waiter = tokio::spawn(async move { rx.recv().await });
        tokio::task::yield_now().await;
        tx.push(5, Some(1)).unwrap();
        assert_eq!(waiter.await.unwrap(), Some(5));
    }
}
