//! The bus carries [`ra_common::Envelope`] — the same wrapper every other
//! `seda-bus` port carries via its own `ra-common` port. Routing is driven by
//! the envelope's `DynamicRoutingSlip`: each hop targets `route().service()`;
//! the slip is walked one hop at a time with `Envelope::ratchet()`.
//!
//! These helpers keep the ergonomic `to` / `payload` / `slip` shape the other
//! ports use on top of the richer `ra_common` type.

use ra_common::serde_json::Value;
pub use ra_common::Envelope;

/// seda-bus routes by service, not operation; `ra_common` still wants a value
/// there.
const OP: &str = "RECEIVE";

/// Build a document envelope addressed to `to`, then visiting each name in
/// `slip` in order.
pub fn make_envelope(
    to: impl Into<String>,
    payload: Option<Value>,
    slip: impl IntoIterator<Item = String>,
) -> Envelope {
    let mut env = Envelope::document();
    // ra-common slips are LIFO: push the itinerary tail-first, then `to`
    // last, so route()/ratchet() yields `to`, then slip[0], slip[1], ...
    let hops: Vec<String> = slip.into_iter().collect();
    for hop in hops.into_iter().rev() {
        env.add_route(hop, OP);
    }
    env.add_route(to.into(), OP);
    if let Some(p) = payload {
        env.add_content(p);
    }
    env
}

/// The channel name the envelope is currently headed for.
pub fn target_service(env: &mut Envelope) -> Option<String> {
    env.route().and_then(|r| r.service()).map(str::to_string)
}

/// The document `CONTENT` value (what [`make_envelope`] stored).
pub fn envelope_payload(env: &Envelope) -> Option<&Value> {
    env.content()
}

pub fn set_payload(env: &mut Envelope, payload: Value) {
    env.add_content(payload);
}
