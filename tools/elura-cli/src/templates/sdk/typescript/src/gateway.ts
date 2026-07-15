import {
  Elr2Frame,
  Elr2ProtocolError,
  FrameKind,
  validateFrame,
} from "./elr2.js";

export const GatewayRoutes = {
  Authenticate: 1,
  Heartbeat: 2,
  Reconnect: 3,
  SessionControl: 4,
  FirstApplication: 100,
} as const;

export interface Identity {
  account_id: number;
  user_id: number;
  region_id: number;
  realm_id: number;
  generation: number;
}

export interface AuthenticateRequest { ticket: string }
export interface AuthenticateResponse { session_id: string; identity: Identity }
export interface ReconnectTicketResponse { ticket: string }
export interface ErrorEnvelope { code: string; message: string; retryable: boolean }

const utf8 = new TextEncoder();
const strictUtf8 = new TextDecoder("utf-8", { fatal: true });

function object(value: unknown, description: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Elr2ProtocolError(`invalid ${description}`);
  }
  return value as Record<string, unknown>;
}

function stringField(value: unknown, name: string): string {
  if (typeof value !== "string") throw new Elr2ProtocolError(`invalid ${name}`);
  return value;
}

function safeInteger(value: unknown, name: string, positive: boolean): number {
  if (!Number.isSafeInteger(value) || (positive && (value as number) <= 0)) {
    throw new Elr2ProtocolError(`invalid ${name}`);
  }
  return value as number;
}

function positiveU32(value: unknown, name: string): number {
  const parsed = safeInteger(value, name, true);
  if (parsed > 0xffff_ffff) throw new Elr2ProtocolError(`invalid ${name}`);
  return parsed;
}

function parseJson(payload: Uint8Array, description: string): unknown {
  try {
    return JSON.parse(strictUtf8.decode(payload));
  } catch (error) {
    throw new Elr2ProtocolError(`invalid ${description}: ${String(error)}`);
  }
}

export function encodeAuthenticateRequest(ticket: string): Uint8Array {
  return utf8.encode(JSON.stringify({ ticket } satisfies AuthenticateRequest));
}

export function decodeAuthenticateResponse(payload: Uint8Array): AuthenticateResponse {
  const value = object(parseJson(payload, "authentication response"), "authentication response");
  const identity = object(value.identity, "identity");
  return {
    session_id: stringField(value.session_id, "session_id"),
    identity: {
      account_id: safeInteger(identity.account_id, "account_id", true),
      user_id: safeInteger(identity.user_id, "user_id", true),
      region_id: positiveU32(identity.region_id, "region_id"),
      realm_id: positiveU32(identity.realm_id, "realm_id"),
      generation: safeInteger(identity.generation, "generation", true),
    },
  };
}

// The Gateway accepts an empty reconnect payload or exactly the UTF-8 bytes for `{}`.
export function encodeReconnectRequest(): Uint8Array {
  return new Uint8Array();
}

export function decodeReconnectResponse(payload: Uint8Array): ReconnectTicketResponse {
  const value = object(parseJson(payload, "reconnect response"), "reconnect response");
  return { ticket: stringField(value.ticket, "ticket") };
}

export function decodeErrorEnvelope(payload: Uint8Array): ErrorEnvelope {
  const value = object(parseJson(payload, "error envelope"), "error envelope");
  const envelope = {
    code: stringField(value.code, "error code"),
    message: stringField(value.message, "error message"),
    retryable: value.retryable,
  };
  if (typeof envelope.retryable !== "boolean" || !/^[A-Z0-9_]{1,64}$/.test(envelope.code) ||
      utf8.encode(envelope.message).byteLength > 1024) {
    throw new Elr2ProtocolError("invalid error envelope fields");
  }
  return envelope as ErrorEnvelope;
}

export function validateClientFrame(
  frame: Elr2Frame,
  authenticated: boolean,
  pendingHeartbeat?: bigint,
): void {
  validateFrame(frame);
  if (frame.kind === FrameKind.Response && frame.route === GatewayRoutes.Heartbeat) {
    if (pendingHeartbeat === undefined || frame.requestId !== pendingHeartbeat ||
        frame.sequence !== 0 || frame.payload.byteLength !== 0) {
      throw new Elr2ProtocolError("heartbeat response does not match an outstanding request");
    }
    return;
  }
  if (frame.kind !== FrameKind.Request) {
    throw new Elr2ProtocolError("Gateway accepts request frames and heartbeat responses only");
  }
  if (frame.route < GatewayRoutes.FirstApplication && frame.sequence !== 0) {
    throw new Elr2ProtocolError("framework requests must have sequence zero");
  }
  const allowed = authenticated
    ? frame.route === GatewayRoutes.Heartbeat || frame.route === GatewayRoutes.Reconnect ||
      frame.route >= GatewayRoutes.FirstApplication
    : frame.route === GatewayRoutes.Authenticate;
  if (!allowed) throw new Elr2ProtocolError("route is not allowed in the current session state");
}

export function heartbeatResponse(request: Elr2Frame): Elr2Frame {
  if (request.kind !== FrameKind.Request || request.route !== GatewayRoutes.Heartbeat ||
      request.sequence !== 0 || request.payload.byteLength !== 0) {
    throw new Elr2ProtocolError("invalid heartbeat request");
  }
  return {
    kind: FrameKind.Response,
    flags: 0,
    route: request.route,
    requestId: request.requestId,
    sequence: 0,
    payload: new Uint8Array(),
  };
}

export enum SessionControlAction {
  Kick = 1,
  AccountVersionChanged = 2,
  DuplicateLogin = 3,
  ForceLogout = 4,
  ServerDraining = 5,
}

export interface SessionControl { action: SessionControlAction; reason: string }

function writeVarint(output: number[], input: bigint): void {
  let value = input;
  while (value >= 0x80n) {
    output.push(Number(value & 0x7fn) | 0x80);
    value >>= 7n;
  }
  output.push(Number(value));
}

function readVarint(input: Uint8Array, cursor: { offset: number }): bigint {
  let value = 0n;
  for (let shift = 0n; shift < 64n && cursor.offset < input.byteLength; shift += 7n) {
    const current = input[cursor.offset++];
    if (current === undefined) break;
    value |= BigInt(current & 0x7f) << shift;
    if ((current & 0x80) === 0) return value;
  }
  throw new Elr2ProtocolError("invalid Session Control protobuf varint");
}

function readLength(input: Uint8Array, cursor: { offset: number }): number {
  const length = readVarint(input, cursor);
  if (length > BigInt(Number.MAX_SAFE_INTEGER) || Number(length) > input.byteLength - cursor.offset) {
    throw new Elr2ProtocolError("truncated Session Control field");
  }
  return Number(length);
}

function skipField(input: Uint8Array, cursor: { offset: number }, wireType: bigint): void {
  let length: number;
  switch (wireType) {
    case 0n: readVarint(input, cursor); return;
    case 1n: length = 8; break;
    case 2n: length = readLength(input, cursor); break;
    case 5n: length = 4; break;
    default: throw new Elr2ProtocolError("unsupported Session Control protobuf wire type");
  }
  if (length > input.byteLength - cursor.offset) {
    throw new Elr2ProtocolError("truncated Session Control field");
  }
  cursor.offset += length;
}

export function encodeSessionControl(control: SessionControl): Uint8Array {
  if (control.action < 1 || control.action > 5 || !Number.isInteger(control.action)) {
    throw new Elr2ProtocolError("unknown Session Control action");
  }
  const reason = utf8.encode(control.reason);
  if (reason.byteLength > 256) {
    throw new Elr2ProtocolError("Session Control reason exceeds 256 bytes");
  }
  const output: number[] = [0x08];
  writeVarint(output, BigInt(control.action));
  if (reason.byteLength !== 0) {
    output.push(0x12);
    writeVarint(output, BigInt(reason.byteLength));
    output.push(...reason);
  }
  return Uint8Array.from(output);
}

export function decodeSessionControl(payload: Uint8Array): SessionControl {
  const cursor = { offset: 0 };
  let action = 0n;
  let reason = "";
  while (cursor.offset < payload.byteLength) {
    const tag = readVarint(payload, cursor);
    const field = tag >> 3n;
    const wireType = tag & 7n;
    if (field === 0n) throw new Elr2ProtocolError("invalid Session Control protobuf tag");
    if (field === 1n && wireType === 0n) {
      action = readVarint(payload, cursor);
    } else if (field === 2n && wireType === 2n) {
      const length = readLength(payload, cursor);
      try {
        reason = strictUtf8.decode(payload.subarray(cursor.offset, cursor.offset + length));
      } catch (error) {
        throw new Elr2ProtocolError(`invalid Session Control UTF-8: ${String(error)}`);
      }
      cursor.offset += length;
    } else {
      skipField(payload, cursor, wireType);
    }
  }
  if (action < 1n || action > 5n) throw new Elr2ProtocolError("unknown Session Control action");
  if (utf8.encode(reason).byteLength > 256) {
    throw new Elr2ProtocolError("Session Control reason exceeds 256 bytes");
  }
  return { action: Number(action) as SessionControlAction, reason };
}
