#![cfg(feature = "identity")]

use async_trait::async_trait;
use elura_core::Result;
use elura_providers::identity::{PasswordCredentialStore, PasswordProvider, Principal};

struct ApplicationPasswordStore;

#[async_trait]
impl PasswordCredentialStore for ApplicationPasswordStore {
    async fn find_password_hash(&self, _username: &str) -> Result<Option<String>> {
        Ok(None)
    }

    async fn create_password_account(
        &self,
        _username: &str,
        _password_hash: &str,
    ) -> Result<Principal> {
        Ok(Principal {
            account_id: 1,
            generation: 1,
        })
    }
}

#[test]
fn application_can_inject_its_password_credential_store() {
    let _provider = PasswordProvider::new(ApplicationPasswordStore).unwrap();
}
