# Elura Gateway protocol for C#

`Elura.Protocol` is a dependency-free .NET 8 implementation of the public Gateway-to-client
ELR2 v{{ELR2_VERSION}} contract. It includes frame encoding, exact-message decoding, TCP stream
reassembly, reserved routes, built-in JSON payloads, standard error envelopes, heartbeat frame
validation, and Session Control protobuf encoding.

It does not open sockets or dispatch application routes.

Run the golden-vector executable:

```sh
dotnet run --project Elura.Protocol.Tests/Elura.Protocol.Tests.csproj
```

For WebSocket transport, send one frame per binary message and negotiate
`{{PROTOCOL_IDENTIFIER}}` as the subprotocol.

Every successful authentication response contains a reconnect ticket. Retain only the latest
ticket, renew it before `ExpiresInSeconds` through the reconnect route, and replace it with the
ticket returned by that response. The renewal request consumes the previous ticket.
