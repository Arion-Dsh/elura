use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use elura_core::{Error, Result};
use tokio::sync::Notify;

#[derive(Default)]
pub(crate) struct DrainController {
    draining: AtomicBool,
    active: AtomicUsize,
    changed: Notify,
}

impl DrainController {
    pub(crate) fn begin(&self) {
        self.draining.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }

    pub(crate) fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }

    pub(crate) fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    pub(crate) fn enter(self: &Arc<Self>) -> Result<ActiveSession> {
        if self.is_draining() {
            return Err(Error::Unavailable);
        }
        self.active.fetch_add(1, Ordering::AcqRel);
        if self.is_draining() {
            self.leave();
            return Err(Error::Unavailable);
        }
        Ok(ActiveSession {
            controller: self.clone(),
        })
    }

    pub(crate) async fn wait_empty(&self) {
        loop {
            if self.active() == 0 {
                return;
            }
            let changed = self.changed.notified();
            if self.active() == 0 {
                return;
            }
            changed.await;
        }
    }

    fn leave(&self) {
        let previous = self.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
        if previous == 1 {
            self.changed.notify_waiters();
        }
    }
}

pub(crate) struct ActiveSession {
    controller: Arc<DrainController>,
}

impl Drop for ActiveSession {
    fn drop(&mut self) {
        self.controller.leave();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn drain_waits_for_existing_sessions_and_rejects_new_ones() {
        let controller = Arc::new(DrainController::default());
        let active = controller.enter().unwrap();
        controller.begin();
        assert!(controller.enter().is_err());
        let waiter = tokio::spawn({
            let controller = controller.clone();
            async move { controller.wait_empty().await }
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        drop(active);
        waiter.await.unwrap();
    }
}
