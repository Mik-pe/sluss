//! Sluss' durable queue and integration state machine.
//!
//! Optional experimental workers can stage and dispatch CI. Queue data and
//! provider observations do not authorize a merge.

pub mod admission;
pub mod batch;
pub mod config;
pub mod coordinator;
pub mod dispatch;
pub mod github;
pub mod integration;
pub mod landing;
pub mod local_refs;
pub mod metrics;
pub mod reconcile;
pub mod scheduler;
pub mod selective;
pub mod store;
pub mod verification;
pub mod webhook;
pub mod worker;
