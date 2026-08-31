//! The unit of work that moves through the bus.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn next_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:x}-{seq:x}")
}

/// A message addressed to a channel.
///
/// `to` is the channel the envelope is currently headed for. `slip` is an
/// ordered list of channels to visit after the current one (a routing slip).
/// `attempts` counts delivery attempts on the current hop.
#[derive(Debug, Clone)]
pub struct Envelope {
    pub id: String,
    pub to: String,
    pub sender: Option<String>,
    pub headers: HashMap<String, String>,
    pub payload: Vec<u8>,
    pub slip: VecDeque<String>,
    pub attempts: u32,
}

impl Envelope {
    pub fn new(to: impl Into<String>, payload: impl Into<Vec<u8>>) -> Self {
        Envelope {
            id: next_id(),
            to: to.into(),
            sender: None,
            headers: HashMap::new(),
            payload: payload.into(),
            slip: VecDeque::new(),
            attempts: 0,
        }
    }

    /// Set the routing slip (channels to visit after the first one).
    pub fn with_slip<I, S>(mut self, hops: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.slip = hops.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_sender(mut self, sender: impl Into<String>) -> Self {
        self.sender = Some(sender.into());
        self
    }

    pub fn with_header(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.headers.insert(key.into(), val.into());
        self
    }

    /// Advance to the next hop in the routing slip. Returns `true` if there was
    /// one (`to` now points at it, `attempts` reset), `false` if complete.
    pub fn advance(&mut self) -> bool {
        match self.slip.pop_front() {
            Some(next) => {
                self.to = next;
                self.attempts = 0;
                true
            }
            None => false,
        }
    }
}
