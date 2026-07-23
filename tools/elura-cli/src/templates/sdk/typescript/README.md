# Elura protocol for TypeScript

`@elura/protocol` is a dependency-free, strict TypeScript implementation of the public
Elura client ELR2 v{{ELR2_VERSION}} contract. It works in browsers and Node.js and includes
frame encoding, TCP stream reassembly, authentication and reconnect payloads, error envelopes,
heartbeat handling, and Session Control protobuf encoding.

It does not open sockets or dispatch application routes.

## Quick start

Authentication creates both the JSON payload and frame. Request IDs can be normal safe integers;
use `bigint` only when the full unsigned 64-bit range is required.

```ts
import { Elr2, EluraProtocol } from "@elura/protocol";

const request = EluraProtocol.authenticate(nextRequestId++, loginTicket);
socket.send(Elr2.encode(request));
```

Application routes start at 100. String payloads are encoded as UTF-8 automatically:

```ts
const request = Elr2.request(100, nextRequestId++, JSON.stringify({ x: 10, y: 20 }));
socket.send(Elr2.encode(request));
```

For WebSocket transport, set `binaryType` to `"arraybuffer"`; `Elr2.decode` accepts either an
`ArrayBuffer` or `Uint8Array` directly:

```ts
socket.binaryType = "arraybuffer";
socket.onmessage = ({ data }) => handle(Elr2.decode(data as ArrayBuffer));
```

TCP and QUIC streams can split or combine frames:

```ts
const decoder = Elr2.stream();
decoder.append(chunk);
for (let frame; (frame = decoder.next()) !== undefined;) {
  handle(frame);
}
```

Reply to a server heartbeat with:

```ts
socket.send(Elr2.encode(EluraProtocol.heartbeatResponse(heartbeat)));
```

Decode a successful authentication response without handling wire-format field names:

```ts
const auth = EluraProtocol.decodeAuthenticate(frame);
console.log(auth.sessionId, auth.identity.userId, auth.reconnect.expiresInSeconds);
```

For WebSocket transport, negotiate `{{PROTOCOL_IDENTIFIER}}` as the subprotocol. Retain only the
latest reconnect ticket, renew it before `expiresInSeconds`, and replace it with the ticket returned
by the renewal response.

Build and run the golden-vector tests:

```sh
npm install
npm test
```
