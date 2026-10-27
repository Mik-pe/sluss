//! Sluss' durable queue and integration state machine.
//!
//! This crate does not yet start CI or publish Git refs. Data in the queue is
//! an observation, not authorization to merge.

pub mod batch;
pub mod github;
pub mod store;
pub mod verification;
pub mod webhook;
