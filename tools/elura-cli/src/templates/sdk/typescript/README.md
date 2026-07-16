# Elura Gateway protocol for TypeScript

This package implements the public Gateway-to-client ELR2 v{{ELR2_VERSION}} contract. It contains
frame encoding, exact-message decoding, TCP stream reassembly, reserved routes, built-in JSON
payloads, standard error envelopes, heartbeat frame validation, and Session Control protobuf
encoding. It does not open sockets or dispatch application routes.

```sh
npm install
npm test
```

For WebSocket transport, send one frame per binary message and negotiate
`{{PROTOCOL_IDENTIFIER}}` as the subprotocol. Request IDs are represented as `bigint` so the full
unsigned 64-bit range is preserved.

Every successful authentication response contains a reconnect ticket. Retain only the latest
ticket, renew it before `expires_in_seconds` through the reconnect route, and replace it with the
ticket returned by that response. The renewal request consumes the previous ticket.
