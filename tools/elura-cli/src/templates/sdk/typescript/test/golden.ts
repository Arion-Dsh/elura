import {
  Elr2,
  Elr2ProtocolError,
  FrameKind,
  EluraProtocol,
  SessionControlAction,
  SessionControlCodec,
} from "../src/index.js";

function assert(condition: unknown, name: string): asserts condition {
  if (!condition) throw new Error(`assertion failed: ${name}`);
}

const request = Elr2.request(100, 7, "hello", 11);
const expected = Uint8Array.from([
  0x45, 0x4c, 0x52, 0x32, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00,
  0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00,
  0x00, 0x0b, 0x00, 0x00, 0x00, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f,
]);
const encoded = Elr2.encode(request);
assert(encoded.byteLength === expected.byteLength &&
  encoded.every((value, index) => value === expected[index]), "ELR2 v2 golden vector");
const decoded = Elr2.decode(encoded);
assert(decoded.route === 100 && decoded.requestId === 7n && Elr2.text(decoded.payload) === "hello",
  "frame round trip");

const response = Elr2.response(request, "ok");
assert(response.kind === FrameKind.Response && response.requestId === request.requestId,
  "response factory");
const push = Elr2.push(101, "news");
assert(push.kind === FrameKind.Push && push.requestId === 0n, "push factory");

const stream = Elr2.stream();
stream.append(encoded.subarray(0, 10));
assert(stream.next() === undefined, "partial stream frame");
stream.append(encoded.subarray(10));
assert(stream.next()?.requestId === 7n && stream.bufferedBytes === 0, "stream completion");

const wrongVersion = encoded.slice();
wrongVersion[5] = 3;
let rejectedWrongVersion = false;
try {
  Elr2.decode(wrongVersion);
} catch (error) {
  rejectedWrongVersion = error instanceof Elr2ProtocolError;
}
assert(rejectedWrongVersion, "wire-version mismatch");

const authRequest = EluraProtocol.authenticate(8, "ticket-value");
assert(authRequest.route === EluraProtocol.routes.authenticate &&
  Elr2.text(authRequest.payload) === "{\"ticket\":\"ticket-value\"}",
  "authentication frame");
const authResponse = Elr2.response(authRequest, JSON.stringify({
  session_id: "session-1",
  identity: { account_id: 1, user_id: 2, region_id: 3, realm_id: 4, generation: 5 },
  reconnect: { ticket: "next-ticket", expires_in_seconds: 60 },
}));
const authenticated = EluraProtocol.decodeAuthenticate(authResponse);
assert(authenticated.sessionId === "session-1" && authenticated.identity.userId === 2 &&
  authenticated.reconnect.expiresInSeconds === 60, "authentication response");

const reconnect = EluraProtocol.renewReconnectTicket(9, "reconnect-value");
assert(reconnect.route === EluraProtocol.routes.renewReconnectTicket &&
  Elr2.text(reconnect.payload) === "{\"ticket\":\"reconnect-value\"}",
  "reconnect renewal frame");

const heartbeat = Elr2.request(EluraProtocol.routes.heartbeat, 10);
const heartbeatReply = EluraProtocol.heartbeatResponse(heartbeat);
assert(heartbeatReply.kind === FrameKind.Response && heartbeatReply.requestId === 10n,
  "heartbeat response");

const control = { action: SessionControlAction.AccountVersionChanged, reason: "credentials rotated" };
const encodedControl = SessionControlCodec.encode(control);
const expectedControl = Uint8Array.from([0x08, 0x02, 0x12, 0x13,
  ...Elr2.utf8("credentials rotated")]);
assert(encodedControl.byteLength === expectedControl.byteLength &&
  encodedControl.every((value, index) => value === expectedControl[index]),
  "Session Control golden vector");
const decodedControl = SessionControlCodec.decode(encodedControl);
assert(decodedControl.action === control.action && decodedControl.reason === control.reason,
  "Session Control protobuf");

console.log("@elura/protocol golden vectors passed.");
