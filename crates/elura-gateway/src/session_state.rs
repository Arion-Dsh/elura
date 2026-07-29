use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};

use elura_core::protocol::{
    Frame, FrameKind, ROUTE_SESSION_CONTROL, SessionControl, SessionControlAction,
};
use elura_core::session::Identity;
use elura_core::{Error, Result};
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct SessionHandle {
    pub(crate) pushes: mpsc::Sender<Frame>,
    pub(crate) disconnect: watch::Sender<bool>,
    pub(crate) authenticated: Arc<AtomicBool>,
}

pub(crate) type SessionSenders = Arc<RwLock<HashMap<Uuid, SessionHandle>>>;
type UserKey = (u32, u32, i64);

#[derive(Default)]
pub(crate) struct SessionIndex {
    pub(crate) identities: HashMap<Uuid, Identity>,
    pub(crate) users: HashMap<UserKey, HashSet<Uuid>>,
}

impl SessionIndex {
    pub(crate) fn insert(&mut self, session_id: Uuid, identity: Identity) {
        if let Some(previous) = self.identities.insert(session_id, identity.clone()) {
            self.remove_user(session_id, &previous);
        }
        self.users
            .entry(identity_key(&identity))
            .or_default()
            .insert(session_id);
    }

    pub(crate) fn remove(&mut self, session_id: Uuid) -> Option<Identity> {
        let identity = self.identities.remove(&session_id)?;
        self.remove_user(session_id, &identity);
        Some(identity)
    }

    fn remove_user(&mut self, session_id: Uuid, identity: &Identity) {
        let key = identity_key(identity);
        if let Some(sessions) = self.users.get_mut(&key) {
            sessions.remove(&session_id);
            if sessions.is_empty() {
                self.users.remove(&key);
            }
        }
    }
}

pub(crate) type SharedSessionIndex = Arc<RwLock<SessionIndex>>;

pub(crate) fn disconnect_handle(handle: SessionHandle, reason: &str) -> Result<()> {
    disconnect_handle_with_action(handle, SessionControlAction::Kick, reason)
}

pub(crate) fn disconnect_handle_with_action(
    handle: SessionHandle,
    action: SessionControlAction,
    reason: &str,
) -> Result<()> {
    let notification = enqueue_session_control(&handle.pushes, action, reason);
    let disconnected = handle.disconnect.send(true).map_err(|_| Error::Unavailable);
    notification.and(disconnected)
}

pub(crate) fn enqueue_session_control(
    pushes: &mpsc::Sender<Frame>,
    action: SessionControlAction,
    reason: &str,
) -> Result<()> {
    let payload = SessionControl::new(action, reason)?.encode_frame_payload()?;
    pushes
        .try_send(Frame {
            kind: FrameKind::Push,
            flags: 0,
            route: ROUTE_SESSION_CONTROL,
            request_id: 0,
            sequence: 0,
            payload,
        })
        .map_err(|_| Error::QueueFull)
}

fn identity_key(identity: &Identity) -> UserKey {
    (identity.region_id, identity.realm_id, identity.user_id)
}
