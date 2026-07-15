import {
  Elr2ProtocolError,
  Elr2StreamDecoder,
  FrameKind,
  SessionControlAction,
  decodeFrame,
  decodeSessionControl,
  encodeAuthenticateRequest,
  encodeFrame,
  encodeSessionControl,
} from "../src/index.js";

function assert(condition: unknown, name: string): asserts condition {
  if (!condition) throw new Error(`assertion failed: ${name}`);
}

const request = {
  kind: FrameKind.Request,
  flags: 0,
  route: 100,
  requestId: 7n,
  sequence: 11,
  payload: new TextEncoder().encode("hello"),
};
const expected = Uint8Array.from([
  0x45, 0x4c, 0x52, 0x32, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00,
  0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00,
  0x00, 0x0b, 0x00, 0x00, 0x00, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f,
]);
const encoded = encodeFrame(request);
assert(encoded.byteLength === expected.byteLength &&
  encoded.every((value, index) => value === expected[index]), "ELR2 v2 golden vector");
const decoded = decodeFrame(encoded);
assert(decoded.route === 100 && decoded.requestId === 7n, "frame round trip");

const stream = new Elr2StreamDecoder();
stream.append(encoded.subarray(0, 10));
assert(stream.next() === undefined, "partial stream frame");
stream.append(encoded.subarray(10));
assert(stream.next()?.requestId === 7n && stream.bufferedBytes === 0, "stream completion");

const wrongVersion = encoded.slice();
wrongVersion[5] = 3;
let rejectedWrongVersion = false;
try {
  decodeFrame(wrongVersion);
} catch (error) {
  rejectedWrongVersion = error instanceof Elr2ProtocolError;
}
assert(rejectedWrongVersion, "wire-version mismatch");

assert(
  new TextDecoder().decode(encodeAuthenticateRequest("ticket-value")) ===
    "{\"ticket\":\"ticket-value\"}",
  "authentication JSON",
);
const control = { action: SessionControlAction.AccountVersionChanged, reason: "credentials rotated" };
const encodedControl = encodeSessionControl(control);
const expectedControl = Uint8Array.from([0x08, 0x02, 0x12, 0x13,
  ...new TextEncoder().encode("credentials rotated")]);
assert(encodedControl.byteLength === expectedControl.byteLength &&
  encodedControl.every((value, index) => value === expectedControl[index]),
  "Session Control golden vector");
const decodedControl = decodeSessionControl(encodedControl);
assert(decodedControl.action === control.action && decodedControl.reason === control.reason,
  "Session Control protobuf");

console.log("@elura/protocol golden vectors passed.");
