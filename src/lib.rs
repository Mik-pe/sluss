//! Sluss' durable queue and integration state machine.
//!
//! The worker does not yet start CI or publish Git refs. Data in the queue is
//! an observation, not authorization to merge.

pub mod admission;
pub mod batch;
pub mod config;
pub mod coordinator;
pub mod github;
pub mod integration;
pub mod metrics;
pub mod reconcile;
pub mod scheduler;
pub mod selective;
pub mod store;
pub mod verification;
pub mod webhook;
pub mod worker;
