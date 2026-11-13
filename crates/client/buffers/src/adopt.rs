//! A buffer topic over a ring, a pool and a hold table the program made itself.
//!
//! A program that serves frames to readers of its own, with no Orion running, makes its ring,
//! pool and hold table with the shared-memory crates. With Orion on, [`Buffers::adopt`] makes
//! that same ring a buffer topic: Orion's subscribers are handed the same ring and the same
//! pool, and their hold marks go into the same table. Nothing is copied and there is one ring.
//!
//! - **The program keeps writing.** It takes buffers, publishes and enforces limits with its
//!   own objects. [`AdoptedBuffers::wake`] after each publish wakes Orion's subscribers.
//! - **Reader slots are shared.** The program says which slots Orion may give its subscribers
//!   (`reader_slots`, a bit for each); its own readers keep the others.
//! - **A comm core that restarts** is handed the same ring and pool again by `poll`.

use std::os::fd::OwnedFd;

use orion_msg_topics::{Field, FieldType, Holds};
use orion_topics::{Publisher, TopicDecl, TopicError};
use shm_ring::Ring;
use shm_ring_buffers::Pool;
use shm_ring_holds::{HoldTable, Limits};

use crate::{Buffers, ENTRY_OVERHEAD, PublisherSide, offer, shared_only};

/// A buffer topic whose ring, pool and hold table belong to the program. Dropping it leaves
/// the topic until the session ends.
pub struct AdoptedBuffers {
    client: orion_connect::Client,
    state: std::rc::Rc<std::cell::RefCell<crate::State>>,
    publisher: Publisher,
    handle: u32,
    offered_on: u64,
    generation: u32,
    reader_slots: u64,
    /// Orion's subscribers in the program's table: slot, marks and limits.
    readers: Vec<(u32, OwnedFd, Limits)>,
}

impl std::fmt::Debug for AdoptedBuffers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AdoptedBuffers({})", self.handle)
    }
}

impl Buffers {
    /// Makes `ring` and `pool`, which the program made and writes, the buffer topic `name`.
    /// `history` and `max_meta` are the declaration's, as for [`Buffers::declare`], and must
    /// be the ring's shape. `reader_slots` are the hold-table slots Orion may give its
    /// subscribers, a bit for each of the 64; zero lets it use any.
    ///
    /// # Errors
    ///
    /// If the session is not on the board, or the comm core refuses: a ring of another shape
    /// is `malformed`.
    pub fn adopt(
        &self,
        name: &str,
        history: u32,
        max_meta: u32,
        ring: &Ring,
        pool: &Pool,
        reader_slots: u64,
    ) -> Result<AdoptedBuffers, TopicError> {
        shared_only(&self.client)?;
        let fields = [Field {
            name: "frame",
            ty: FieldType::Buffers,
            unit: "",
            type_name: "",
        }];
        let publisher = self.topics.adopt(
            &TopicDecl {
                name,
                fields: &fields,
                history,
                expiry_ms: 0,
                max_payload: ENTRY_OVERHEAD + max_meta,
            },
            ring,
        )?;
        let handle = publisher.handle();
        self.state.borrow_mut().publishers.push(PublisherSide {
            handle,
            changes: Vec::new(),
        });
        let offered_on = offer(&self.client, handle, pool, reader_slots)?;
        Ok(AdoptedBuffers {
            client: self.client.clone(),
            state: self.state.clone(),
            publisher,
            handle,
            offered_on,
            generation: pool.generation(),
            reader_slots,
            readers: Vec::new(),
        })
    }
}

impl AdoptedBuffers {
    /// Looks at what the comm core has sent: Orion's subscribers that came with their marks,
    /// put in `table`, or went, taken out of it. Offers `pool` again when it is a new one or
    /// the comm core is. A subscriber gets no frames until this has run, so call it once a
    /// frame or whenever the program is idle. It does not wait.
    ///
    /// # Errors
    ///
    /// If the connection broke, or a subscriber's marks cannot be put in `table`.
    pub fn poll(&mut self, pool: &Pool, table: &mut HoldTable) -> Result<(), TopicError> {
        self.client.drain()?;
        let now = self.publisher.handle();
        let connection = self.client.session(|session| session.reconnects());
        if now != self.handle
            || connection != self.offered_on
            || pool.generation() != self.generation
        {
            if let Some(side) = self
                .state
                .borrow_mut()
                .publishers
                .iter_mut()
                .find(|side| side.handle == self.handle)
            {
                side.handle = now;
            }
            self.handle = now;
            self.offered_on = offer(&self.client, self.handle, pool, self.reader_slots)?;
            self.generation = pool.generation();
        }
        let changes = {
            let mut state = self.state.borrow_mut();
            match state
                .publishers
                .iter_mut()
                .find(|side| side.handle == self.handle)
            {
                Some(side) if !side.changes.is_empty() => std::mem::take(&mut side.changes),
                _ => return Ok(()),
            }
        };
        for change in changes {
            table.remove(change.slot, pool.page());
            self.readers.retain(|(slot, _, _)| *slot != change.slot);
            let Some(page) = change.page else {
                continue;
            };
            let keep = page
                .try_clone()
                .map_err(|error| TopicError::Client(error.into()))?;
            table.add(change.slot, page, change.limits)?;
            self.readers.push((change.slot, keep, change.limits));
            // Only now is the subscriber given the pool: it never reads a frame the program
            // is not yet protecting.
            self.client.send(&Holds {
                handle: self.handle,
                subscriber: change.slot,
                max_held: change.limits.max_held,
                max_hold_ms: change.max_hold_ms,
                present: true,
            })?;
        }
        Ok(())
    }

    /// Puts Orion's subscribers in `table` again, for a program that made a new table, as it
    /// does with a new pool.
    ///
    /// # Errors
    ///
    /// If a subscriber's marks cannot be mapped again, or its slot is taken in `table`.
    pub fn readmit(&self, table: &mut HoldTable) -> Result<(), TopicError> {
        for (slot, page, limits) in &self.readers {
            let page = page
                .try_clone()
                .map_err(|error| TopicError::Client(error.into()))?;
            table.add(*slot, page, *limits)?;
        }
        Ok(())
    }

    /// Wakes Orion's subscribers that are asleep, after the program published. No system call
    /// for one that is awake.
    pub fn wake(&mut self) {
        self.publisher.wake();
    }

    /// How many of Orion's subscribers the topic has.
    #[must_use]
    pub fn subscribers(&self) -> u32 {
        self.publisher.subscribers()
    }

    /// What the newest of Orion's subscribers asked for, as it sent it.
    pub fn with_request<R>(&self, with: impl FnOnce(&[u8]) -> R) -> R {
        self.publisher.with_request(with)
    }
}
