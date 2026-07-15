use uuid::Uuid;

pub fn new_trace_id() -> String {
    Uuid::new_v4().simple().to_string()
}

pub fn ensure_trace_id(candidate: &str) -> String {
    if candidate.len() == 32 && candidate.bytes().all(|value| value.is_ascii_hexdigit()) {
        candidate.to_ascii_lowercase()
    } else {
        new_trace_id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_valid_trace_and_replaces_invalid_value() {
        let valid = "0123456789abcdef0123456789abcdef";
        assert_eq!(ensure_trace_id(valid), valid);
        assert_ne!(ensure_trace_id("broken"), "broken");
        assert_eq!(new_trace_id().len(), 32);
    }
}
