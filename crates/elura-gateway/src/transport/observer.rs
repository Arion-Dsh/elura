use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use elura_core::Result;
use elura_core::session::{Session, SessionSnapshot};
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEventKind {
    Connected,
    Authenticated,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionEvent {
    pub kind: SessionEventKind,
    pub session: SessionSnapshot,
}

/// Receives immutable Session lifecycle events.
///
/// Observers execute outside Session locks and must return quickly. Failures
/// are logged and isolated from the connection and from other observers.
pub trait SessionObserver: Send + Sync + 'static {
    fn observe(&self, event: SessionEvent) -> Result<()>;
}

impl<F> SessionObserver for F
where
    F: Fn(SessionEvent) -> Result<()> + Send + Sync + 'static,
{
    fn observe(&self, event: SessionEvent) -> Result<()> {
        self(event)
    }
}

pub(crate) fn notify(
    observers: &[Arc<dyn SessionObserver>],
    kind: SessionEventKind,
    session: &Session,
) {
    if observers.is_empty() {
        return;
    }
    let snapshot = match session.snapshot() {
        Ok(snapshot) => snapshot,
        Err(error) => {
            warn!(session_id = %session.id(), %error, "could not snapshot Session for observers");
            return;
        }
    };
    for observer in observers {
        let event = SessionEvent {
            kind,
            session: snapshot.clone(),
        };
        match catch_unwind(AssertUnwindSafe(|| observer.observe(event))) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => warn!(
                session_id = %snapshot.id,
                ?kind,
                %error,
                "Session observer failed"
            ),
            Err(_) => warn!(
                session_id = %snapshot.id,
                ?kind,
                "Session observer panicked"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    #[test]
    fn a_panicking_observer_does_not_hide_the_event_from_others() {
        let called = Arc::new(AtomicBool::new(false));
        let marker = called.clone();
        let observers: Vec<Arc<dyn SessionObserver>> = vec![
            Arc::new(|_event: SessionEvent| -> Result<()> { panic!("observer failed") }),
            Arc::new(move |_event: SessionEvent| {
                marker.store(true, Ordering::Release);
                Ok(())
            }),
        ];
        let session = Session::new("192.0.2.10".parse().unwrap());

        notify(&observers, SessionEventKind::Connected, &session);

        assert!(called.load(Ordering::Acquire));
    }
}
