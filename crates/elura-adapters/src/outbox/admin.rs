use std::sync::Arc;
use std::time::SystemTime;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use chrono::{DateTime, Utc};
use elura_core::Error;
use serde::Deserialize;
use uuid::Uuid;

use super::OutboxStore;

#[derive(Debug, Clone)]
pub struct OutboxAdminConfig {
    pub default_limit: usize,
    pub max_limit: usize,
    pub max_request_bytes: usize,
}

impl Default for OutboxAdminConfig {
    fn default() -> Self {
        Self {
            default_limit: 50,
            max_limit: 1000,
            max_request_bytes: 4096,
        }
    }
}

#[derive(Clone)]
pub struct OutboxAdmin {
    state: AdminState,
}

#[derive(Clone)]
struct AdminState {
    store: Arc<dyn OutboxStore>,
    config: OutboxAdminConfig,
}

impl OutboxAdmin {
    pub fn new(store: Arc<dyn OutboxStore>, config: OutboxAdminConfig) -> elura_core::Result<Self> {
        if config.default_limit == 0
            || config.default_limit > config.max_limit
            || config.max_limit > 10_000
            || config.max_request_bytes == 0
        {
            return Err(Error::InvalidConfig("invalid Outbox admin limits".into()));
        }
        Ok(Self {
            state: AdminState { store, config },
        })
    }

    /// Routes are relative so upper projects can mount this under their
    /// authenticated administration namespace.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/dead-letters", get(list_dead_letters))
            .route("/dead-letters/{id}/replay", post(replay_dead_letter))
            .with_state(self.state.clone())
    }
}

#[derive(Deserialize)]
struct ListQuery {
    limit: Option<usize>,
}

async fn list_dead_letters(
    State(state): State<AdminState>,
    Query(query): Query<ListQuery>,
) -> Response {
    let limit = query.limit.unwrap_or(state.config.default_limit);
    if limit == 0 || limit > state.config.max_limit {
        return (StatusCode::BAD_REQUEST, "limit is out of range").into_response();
    }
    match state.store.list_dead_letters(limit).await {
        Ok(items) => {
            let mut response = axum::Json(items).into_response();
            response
                .headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            response
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "list dead letters").into_response(),
    }
}

#[derive(Default, Deserialize)]
struct ReplayRequest {
    available_at: Option<DateTime<Utc>>,
}

async fn replay_dead_letter(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    body: Bytes,
) -> Response {
    if body.len() > state.config.max_request_bytes {
        return (StatusCode::PAYLOAD_TOO_LARGE, "replay request is too large").into_response();
    }
    let Ok(id) = Uuid::parse_str(id.trim()) else {
        return (StatusCode::BAD_REQUEST, "valid event ID is required").into_response();
    };
    let input = if body.is_empty() {
        ReplayRequest::default()
    } else {
        match serde_json::from_slice(&body) {
            Ok(input) => input,
            Err(_) => return (StatusCode::BAD_REQUEST, "invalid replay request").into_response(),
        }
    };
    let available_at = input.available_at.map_or_else(SystemTime::now, Into::into);
    match state.store.replay_dead_letter(id, available_at).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(Error::OutboxNotFound) => {
            (StatusCode::NOT_FOUND, "dead letter not found").into_response()
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "replay dead letter").into_response(),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::outbox::{MemoryOutbox, OutboxEvent};

    #[tokio::test]
    async fn lists_and_replays_dead_letters() {
        let store = Arc::new(MemoryOutbox::default());
        let event = OutboxEvent::new("mail", vec![1]).unwrap();
        store.append(event.clone()).await.unwrap();
        let delivery = store
            .acquire("worker", 1, Duration::from_secs(5))
            .await
            .unwrap()
            .remove(0);
        store.dead_letter(&delivery, "failed").await.unwrap();
        let admin = OutboxAdmin::new(store.clone(), OutboxAdminConfig::default()).unwrap();
        let listed = list_dead_letters(
            State(admin.state.clone()),
            Query(ListQuery { limit: Some(10) }),
        )
        .await;
        assert_eq!(listed.status(), StatusCode::OK);
        let replayed =
            replay_dead_letter(State(admin.state), Path(event.id.to_string()), Bytes::new()).await;
        assert_eq!(replayed.status(), StatusCode::NO_CONTENT);
        assert!(store.list_dead_letters(10).await.unwrap().is_empty());
    }
}
