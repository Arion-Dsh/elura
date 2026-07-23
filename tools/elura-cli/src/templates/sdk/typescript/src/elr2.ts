export const ELR2_MAGIC = 0x454c5232;
export const ELR2_VERSION = {{ELR2_VERSION}};
export const ELR2_HEADER_LENGTH = 28;
export const DEFAULT_MAX_PAYLOAD = 1 << 20;
export const ABSOLUTE_MAX_PAYLOAD = 64 << 20;
export const PROTOCOL_IDENTIFIER = "{{PROTOCOL_IDENTIFIER}}";

export enum FrameKind {
  Request = 1,
  Response = 2,
  Push = 3,
  Error = 4,
}

export interface Elr2Frame {
  kind: FrameKind;
  flags: number;
  route: number;
  requestId: bigint;
  sequence: number;
  payload: Uint8Array;
}

export type ByteInput = Uint8Array | ArrayBuffer;
export type FramePayload = Uint8Array | string;
export type RequestId = number | bigint;

export class Elr2ProtocolError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "Elr2ProtocolError";
  }
}

function validateLimit(maxPayload: number): void {
  if (!Number.isInteger(maxPayload) || maxPayload <= 0 || maxPayload > ABSOLUTE_MAX_PAYLOAD) {
    throw new RangeError("max payload must be in 1..=64MiB");
  }
}

function validU32(value: number): boolean {
  return Number.isInteger(value) && value >= 0 && value <= 0xffff_ffff;
}

const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder();

function bytes(input: ByteInput): Uint8Array {
  return input instanceof Uint8Array ? input : new Uint8Array(input);
}

function payloadBytes(payload: FramePayload): Uint8Array {
  return typeof payload === "string" ? utf8Encoder.encode(payload) : payload;
}

function requestId(value: RequestId): bigint {
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new RangeError("request id number must be a non-negative safe integer");
    }
    return BigInt(value);
  }
  if (value < 0n || value > 0xffff_ffff_ffff_ffffn) {
    throw new RangeError("request id must be a uint64");
  }
  return value;
}

export function encodeUtf8(text: string): Uint8Array {
  return utf8Encoder.encode(text);
}

export function decodeUtf8(input: ByteInput): string {
  return utf8Decoder.decode(bytes(input));
}

export function requestFrame(
  route: number,
  id: RequestId,
  payload: FramePayload = new Uint8Array(),
  sequence = 0,
): Elr2Frame {
  return createFrame(FrameKind.Request, route, requestId(id), sequence, payload);
}

export function responseFrame(
  request: Elr2Frame,
  payload: FramePayload = new Uint8Array(),
): Elr2Frame {
  requireRequest(request);
  return createFrame(FrameKind.Response, request.route, request.requestId, request.sequence, payload);
}

export function errorFrame(
  request: Elr2Frame,
  payload: FramePayload = new Uint8Array(),
): Elr2Frame {
  requireRequest(request);
  return createFrame(FrameKind.Error, request.route, request.requestId, request.sequence, payload);
}

export function pushFrame(
  route: number,
  payload: FramePayload = new Uint8Array(),
  sequence = 0,
): Elr2Frame {
  return createFrame(FrameKind.Push, route, 0n, sequence, payload);
}

function createFrame(
  kind: FrameKind,
  route: number,
  id: bigint,
  sequence: number,
  payload: FramePayload,
): Elr2Frame {
  const frame = { kind, flags: 0, route, requestId: id, sequence, payload: payloadBytes(payload) };
  validateFrame(frame, ABSOLUTE_MAX_PAYLOAD);
  return frame;
}

function requireRequest(frame: Elr2Frame): void {
  if (frame.kind !== FrameKind.Request) {
    throw new Elr2ProtocolError("response source must be a request frame");
  }
}

export function validateFrame(frame: Elr2Frame, maxPayload = DEFAULT_MAX_PAYLOAD): void {
  validateLimit(maxPayload);
  if (!(frame.payload instanceof Uint8Array) || frame.payload.byteLength > maxPayload) {
    throw new Elr2ProtocolError("payload is too large");
  }
  if (frame.flags !== 0) throw new Elr2ProtocolError("unsupported frame flags");
  if (!validU32(frame.route) || frame.route === 0) {
    throw new Elr2ProtocolError("route must be a non-zero uint32");
  }
  if (!validU32(frame.sequence)) throw new Elr2ProtocolError("sequence must be a uint32");
  if (![FrameKind.Request, FrameKind.Response, FrameKind.Push, FrameKind.Error].includes(frame.kind)) {
    throw new Elr2ProtocolError("unknown frame kind");
  }
  if (frame.requestId < 0n || frame.requestId > 0xffff_ffff_ffff_ffffn) {
    throw new Elr2ProtocolError("request id must be a uint64");
  }
  if (frame.kind === FrameKind.Push) {
    if (frame.requestId !== 0n) throw new Elr2ProtocolError("push request id must be zero");
  } else if (frame.requestId === 0n) {
    throw new Elr2ProtocolError("request id must be non-zero");
  }
}

export function encodeFrame(frame: Elr2Frame, maxPayload = DEFAULT_MAX_PAYLOAD): Uint8Array {
  validateFrame(frame, maxPayload);
  const output = new Uint8Array(ELR2_HEADER_LENGTH + frame.payload.byteLength);
  const view = new DataView(output.buffer);
  view.setUint32(0, ELR2_MAGIC);
  view.setUint16(4, ELR2_VERSION);
  view.setUint8(6, frame.kind);
  view.setUint8(7, frame.flags);
  view.setUint32(8, frame.route);
  view.setBigUint64(12, frame.requestId);
  view.setUint32(20, frame.sequence);
  view.setUint32(24, frame.payload.byteLength);
  output.set(frame.payload, ELR2_HEADER_LENGTH);
  return output;
}

export function decodeFrame(input: ByteInput, maxPayload = DEFAULT_MAX_PAYLOAD): Elr2Frame {
  const message = bytes(input);
  validateLimit(maxPayload);
  if (message.byteLength < ELR2_HEADER_LENGTH) {
    throw new Elr2ProtocolError("incomplete Elura frame");
  }
  const view = new DataView(message.buffer, message.byteOffset, message.byteLength);
  if (view.getUint32(0) !== ELR2_MAGIC) throw new Elr2ProtocolError("invalid Elura magic");
  if (view.getUint16(4) !== ELR2_VERSION) {
    throw new Elr2ProtocolError("unsupported Elura version");
  }
  const payloadLength = view.getUint32(24);
  if (payloadLength > maxPayload) throw new Elr2ProtocolError("Elura payload is too large");
  if (message.byteLength !== ELR2_HEADER_LENGTH + payloadLength) {
    throw new Elr2ProtocolError("Elura message must contain exactly one frame");
  }
  const frame: Elr2Frame = {
    kind: view.getUint8(6) as FrameKind,
    flags: view.getUint8(7),
    route: view.getUint32(8),
    requestId: view.getBigUint64(12),
    sequence: view.getUint32(20),
    payload: message.subarray(ELR2_HEADER_LENGTH),
  };
  validateFrame(frame, maxPayload);
  return frame;
}

export class Elr2StreamDecoder {
  private buffer = new Uint8Array();

  constructor(private readonly maxPayload = DEFAULT_MAX_PAYLOAD) {
    validateLimit(maxPayload);
  }

  get bufferedBytes(): number {
    return this.buffer.byteLength;
  }

  append(input: ByteInput): void {
    const chunk = bytes(input);
    const joined = new Uint8Array(this.buffer.byteLength + chunk.byteLength);
    joined.set(this.buffer);
    joined.set(chunk, this.buffer.byteLength);
    this.buffer = joined;
  }

  next(): Elr2Frame | undefined {
    if (this.buffer.byteLength < ELR2_HEADER_LENGTH) return undefined;
    const header = new DataView(this.buffer.buffer, this.buffer.byteOffset, ELR2_HEADER_LENGTH);
    if (header.getUint32(0) !== ELR2_MAGIC) throw new Elr2ProtocolError("invalid Elura magic");
    if (header.getUint16(4) !== ELR2_VERSION) {
      throw new Elr2ProtocolError("unsupported Elura version");
    }
    const payloadLength = header.getUint32(24);
    if (payloadLength > this.maxPayload) throw new Elr2ProtocolError("Elura payload is too large");
    const total = ELR2_HEADER_LENGTH + payloadLength;
    if (this.buffer.byteLength < total) return undefined;
    const frame = decodeFrame(this.buffer.subarray(0, total), this.maxPayload);
    this.buffer = this.buffer.slice(total);
    return frame;
  }
}

export const Elr2 = {
  version: ELR2_VERSION,
  protocolIdentifier: PROTOCOL_IDENTIFIER,
  request: requestFrame,
  response: responseFrame,
  error: errorFrame,
  push: pushFrame,
  encode: encodeFrame,
  decode: decodeFrame,
  stream: (maxPayload = DEFAULT_MAX_PAYLOAD) => new Elr2StreamDecoder(maxPayload),
  utf8: encodeUtf8,
  text: decodeUtf8,
} as const;
