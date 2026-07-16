//! Shared process infrastructure for Elura server runtimes.

#![deny(rustdoc::broken_intra_doc_links)]
#![deny(missing_docs)]

/// Serializable launch-time configuration shared by server processes.
pub mod launch;
pub mod lifecycle;
pub mod observability;
pub mod outbox;
pub mod security;
