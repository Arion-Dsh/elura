use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Instant, SystemTime};

use elura_core::Error;
use serde::Serialize;

pub(crate) struct GatewayStats {
    started_at: SystemTime,
    started: Instant,
    connections: AtomicU64,
    active_connections: AtomicI64,
    authenticated_sessions: AtomicI64,
    requests: AtomicU64,
    rejected: AtomicU64,
    failures: AtomicU64,
    pushes: AtomicU64,
    push_failures: AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
pub struct GatewayStatsSnapshot {
    pub started_at: SystemTime,
    pub uptime_millis: u64,
    pub connections: u64,
    pub active_connections: i64,
    pub authenticated_sessions: i64,
    pub requests: u64,
    pub rejected: u64,
    pub failures: u64,
    pub pushes: u64,
    pub push_failures: u64,
}

impl Default for GatewayStats {
    fn default() -> Self {
        Self {
            started_at: SystemTime::now(),
            started: Instant::now(),
            connections: AtomicU64::new(0),
            active_connections: AtomicI64::new(0),
            authenticated_sessions: AtomicI64::new(0),
            requests: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            pushes: AtomicU64::new(0),
            push_failures: AtomicU64::new(0),
        }
    }
}

impl GatewayStats {
    pub(crate) fn connection_started(&self) -> ActiveGatewayConnection<'_> {
        self.connections.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        ActiveGatewayConnection(self)
    }

    pub(crate) fn authenticated_session_started(&self) {
        self.authenticated_sessions.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn authenticated_session_ended(&self) {
        self.authenticated_sessions.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn record_request(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_rejection(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_failure(&self) {
        self.failures.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_push(&self) {
        self.pushes.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_push_failure(&self) {
        self.push_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_error(&self, error: &Error) {
        match error {
            Error::Authentication | Error::RateLimited | Error::Business { .. } => {
                self.record_rejection();
            }
            _ => self.record_failure(),
        }
    }

    pub(crate) fn snapshot(&self) -> GatewayStatsSnapshot {
        GatewayStatsSnapshot {
            started_at: self.started_at,
            uptime_millis: self.started.elapsed().as_millis() as u64,
            connections: self.connections.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            authenticated_sessions: self.authenticated_sessions.load(Ordering::Relaxed),
            requests: self.requests.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            pushes: self.pushes.load(Ordering::Relaxed),
            push_failures: self.push_failures.load(Ordering::Relaxed),
        }
    }
}

pub(crate) struct ActiveGatewayConnection<'a>(&'a GatewayStats);

impl Drop for ActiveGatewayConnection<'_> {
    fn drop(&mut self) {
        self.0.active_connections.fetch_sub(1, Ordering::Relaxed);
    }
}
