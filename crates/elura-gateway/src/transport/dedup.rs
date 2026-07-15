use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use elura_core::protocol::Frame;
use elura_core::{Error, Result};
use sha2::{Digest, Sha256};

#[derive(Clone, PartialEq, Eq)]
struct RequestFingerprint {
    route: u32,
    payload_hash: [u8; 32],
}

impl RequestFingerprint {
    fn new(request: &Frame) -> Self {
        Self {
            route: request.route,
            payload_hash: Sha256::digest(&request.payload).into(),
        }
    }
}

pub(crate) struct ResponseCache {
    ttl: Duration,
    capacity: usize,
    max_bytes: usize,
    used_bytes: usize,
    entries: HashMap<u64, (RequestFingerprint, Frame, Instant)>,
    order: VecDeque<(u64, Instant)>,
}

impl ResponseCache {
    pub(crate) fn new(ttl: Duration, capacity: usize, max_bytes: usize) -> Self {
        Self {
            ttl,
            capacity,
            max_bytes,
            used_bytes: 0,
            entries: HashMap::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
        }
    }

    pub(crate) fn get(&mut self, request: &Frame) -> Result<Option<Frame>> {
        self.prune(Instant::now());
        let Some((fingerprint, frame, _)) = self.entries.get(&request.request_id) else {
            return Ok(None);
        };
        if *fingerprint != RequestFingerprint::new(request) {
            return Err(Error::InvalidFrame(
                "request ID was reused with a different route or payload".into(),
            ));
        }
        Ok(Some(frame.clone()))
    }

    pub(crate) fn insert(&mut self, request: &Frame, response: Frame) {
        let response_bytes = response.payload.len();
        if request.request_id == 0
            || self.capacity == 0
            || self.ttl.is_zero()
            || response_bytes > self.max_bytes
        {
            return;
        }
        let now = Instant::now();
        self.prune(now);
        if self.entries.contains_key(&request.request_id) {
            return;
        }
        while self.entries.len() >= self.capacity
            || self.used_bytes.saturating_add(response_bytes) > self.max_bytes
        {
            if !self.evict_oldest() {
                return;
            }
        }
        let request_id = request.request_id;
        let expires_at = now + self.ttl;
        self.entries.insert(
            request_id,
            (RequestFingerprint::new(request), response, expires_at),
        );
        self.used_bytes += response_bytes;
        self.order.push_back((request_id, expires_at));
    }

    fn prune(&mut self, now: Instant) {
        while self
            .order
            .front()
            .is_some_and(|(_, expires_at)| *expires_at <= now)
        {
            let Some((request_id, expires_at)) = self.order.pop_front() else {
                break;
            };
            if self
                .entries
                .get(&request_id)
                .is_some_and(|(_, _, current)| *current == expires_at)
                && let Some((_, frame, _)) = self.entries.remove(&request_id)
            {
                self.used_bytes = self.used_bytes.saturating_sub(frame.payload.len());
            }
        }
    }

    fn evict_oldest(&mut self) -> bool {
        while let Some((request_id, expires_at)) = self.order.pop_front() {
            if self
                .entries
                .get(&request_id)
                .is_some_and(|(_, _, current)| *current == expires_at)
            {
                if let Some((_, frame, _)) = self.entries.remove(&request_id) {
                    self.used_bytes = self.used_bytes.saturating_sub(frame.payload.len());
                }
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use elura_core::protocol::Frame;

    use super::*;

    #[test]
    fn evicts_oldest_response_at_capacity() {
        let mut cache = ResponseCache::new(Duration::from_secs(1), 1, 1024);
        let one = Frame::request(100, 1, Bytes::new()).unwrap();
        let two = Frame::request(100, 2, Bytes::new()).unwrap();
        cache.insert(&one, Frame::response(&one, Bytes::from_static(b"one")));
        cache.insert(&two, Frame::response(&two, Bytes::from_static(b"two")));
        assert!(cache.get(&one).unwrap().is_none());
        assert_eq!(
            cache.get(&two).unwrap().unwrap().payload,
            Bytes::from_static(b"two")
        );
    }

    #[test]
    fn rejects_request_id_reuse_with_different_content() {
        let mut cache = ResponseCache::new(Duration::from_secs(1), 1, 1024);
        let original = Frame::request(100, 1, Bytes::from_static(b"one")).unwrap();
        cache.insert(&original, Frame::response(&original, Bytes::new()));
        let changed = Frame::request(101, 1, Bytes::from_static(b"two")).unwrap();
        assert!(matches!(cache.get(&changed), Err(Error::InvalidFrame(_))));
    }

    #[test]
    fn enforces_response_byte_budget() {
        let mut cache = ResponseCache::new(Duration::from_secs(1), 8, 3);
        let request = Frame::request(100, 1, Bytes::from_static(b"large request")).unwrap();
        cache.insert(
            &request,
            Frame::response(&request, Bytes::from_static(b"four")),
        );
        assert!(cache.get(&request).unwrap().is_none());
    }
}
