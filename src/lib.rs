//! Sluss' durable queue and integration state machine.
//!
//! Optional experimental workers can stage and dispatch CI. Queue data and
//! provider observations do not authorize a merge.

mod adaptive;
pub mod admission;
pub mod batch;
pub mod benchmark;
pub mod config;
pub mod coordinator;
mod database;
pub mod dispatch;
pub mod github;
pub mod integration;
pub mod landing;
pub mod local_refs;
pub mod metrics;
pub mod reconcile;
pub mod scheduler;
pub mod selective;
pub mod setup;
pub mod store;
pub mod verification;
pub mod webhook;
pub mod worker;

mod workflow;
