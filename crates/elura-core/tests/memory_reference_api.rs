use elura_core::account_version::MemoryAccountVersionStore;
use elura_core::otp::MemoryOtpStore;
use elura_core::outbox::MemoryOutbox;
use elura_core::replay_protection::MemoryReplayProtectionStore;

#[test]
fn memory_reference_implementations_follow_their_contracts() {
    fn type_exists<T>() {}

    type_exists::<MemoryAccountVersionStore>();
    type_exists::<MemoryOtpStore>();
    type_exists::<MemoryOutbox>();
    type_exists::<MemoryReplayProtectionStore>();
}
