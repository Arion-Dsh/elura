# elura-providers

`elura-providers` contains object-safe integration contracts and hardened built-in identity,
notification, OTP, and payment providers. Applications own account and order persistence; this
crate owns provider protocol validation, signature verification, and normalized results.

Provider implementations are opt-in Cargo features. The base crate always exposes payment
contracts and registries, while identity, OTP, and concrete channels remain feature-gated.

## Payment contracts

Use `PaymentRegistry` as the application boundary. Its operation methods enforce capabilities,
validate caller input, invoke the selected provider, and validate the provider response.

```rust
use elura_providers::payment::{CheckoutRequest, Money, PaymentLookup};

let amount = Money::new("CNY", 1_900)?;
let checkout = CheckoutRequest::new("order-2026-001", amount, "Starter pack");
let lookup = PaymentLookup::merchant("order-2026-001");

# Ok::<(), elura_providers::ProviderError>(())
```

Provider-specific checkout data belongs in `CheckoutRequest::options`, normally populated with
`CheckoutRequest::with_provider_options`. Built-in options types such as
`AlipayCheckoutOptions` and `WechatMiniCheckoutOptions` make those schemas explicit.

Callback adapters should construct `NotificationRequest` from the original HTTP method, URI,
headers, and bytes. `PaymentNotificationVerifier` combines signature verification with durable
replay protection.

## Identity contracts

`IdentityRegistry` stores object-safe providers and `IdentityService` connects them to the
application-owned `elura_core::identity::IdentityBindingStore`. The binding contract lives in
`elura-core` so infrastructure adapters and providers remain independent sibling crates. Built-in
credentials are public serializable types, including `PasswordCredential`, `PhoneCredential`,
`OAuth2Credential`, and `CodeCredential`. Custom providers may continue to use their own JSON
credential schema at the trait boundary.

## Errors

`ProviderError::code` is stable and suitable for transport mapping. `is_retryable` and
`retry_after` distinguish temporary upstream failures from invalid requests and rejected
credentials. Applications should avoid returning raw provider rejection messages to untrusted
clients.
