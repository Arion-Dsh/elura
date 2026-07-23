#include "elura/elr2.hpp"

#include <algorithm>
#include <limits>
#include <utility>

namespace elura {
namespace {

std::uint16_t read_u16(std::span<const std::uint8_t> bytes) {
  return static_cast<std::uint16_t>((static_cast<std::uint16_t>(bytes[0]) << 8U) |
                                    static_cast<std::uint16_t>(bytes[1]));
}

std::uint32_t read_u32(std::span<const std::uint8_t> bytes) {
  return (static_cast<std::uint32_t>(bytes[0]) << 24U) |
         (static_cast<std::uint32_t>(bytes[1]) << 16U) |
         (static_cast<std::uint32_t>(bytes[2]) << 8U) |
         static_cast<std::uint32_t>(bytes[3]);
}

std::uint64_t read_u64(std::span<const std::uint8_t> bytes) {
  return (static_cast<std::uint64_t>(read_u32(bytes)) << 32U) | read_u32(bytes.subspan(4));
}

void write_u16(Bytes& output, std::size_t offset, std::uint16_t value) {
  output[offset] = static_cast<std::uint8_t>(value >> 8U);
  output[offset + 1] = static_cast<std::uint8_t>(value);
}

void write_u32(Bytes& output, std::size_t offset, std::uint32_t value) {
  output[offset] = static_cast<std::uint8_t>(value >> 24U);
  output[offset + 1] = static_cast<std::uint8_t>(value >> 16U);
  output[offset + 2] = static_cast<std::uint8_t>(value >> 8U);
  output[offset + 3] = static_cast<std::uint8_t>(value);
}

void write_u64(Bytes& output, std::size_t offset, std::uint64_t value) {
  write_u32(output, offset, static_cast<std::uint32_t>(value >> 32U));
  write_u32(output, offset + 4, static_cast<std::uint32_t>(value));
}

FrameKind parse_kind(std::uint8_t value) {
  switch (value) {
    case 1: return FrameKind::Request;
    case 2: return FrameKind::Response;
    case 3: return FrameKind::Push;
    case 4: return FrameKind::Error;
    default: throw Elr2ProtocolError("unknown frame kind");
  }
}

bool valid_kind(FrameKind kind) {
  switch (kind) {
    case FrameKind::Request:
    case FrameKind::Response:
    case FrameKind::Push:
    case FrameKind::Error: return true;
  }
  return false;
}

bool valid_utf8(const std::string& value) {
  const auto* bytes = reinterpret_cast<const std::uint8_t*>(value.data());
  std::size_t index = 0;
  while (index < value.size()) {
    const auto first = bytes[index++];
    if (first <= 0x7fU) continue;
    std::uint32_t codepoint;
    std::size_t continuation;
    std::uint32_t minimum;
    if ((first & 0xe0U) == 0xc0U) {
      codepoint = first & 0x1fU;
      continuation = 1;
      minimum = 0x80U;
    } else if ((first & 0xf0U) == 0xe0U) {
      codepoint = first & 0x0fU;
      continuation = 2;
      minimum = 0x800U;
    } else if ((first & 0xf8U) == 0xf0U) {
      codepoint = first & 0x07U;
      continuation = 3;
      minimum = 0x10000U;
    } else {
      return false;
    }
    if (continuation > value.size() - index) return false;
    for (std::size_t count = 0; count < continuation; ++count) {
      const auto next = bytes[index++];
      if ((next & 0xc0U) != 0x80U) return false;
      codepoint = (codepoint << 6U) | (next & 0x3fU);
    }
    if (codepoint < minimum || codepoint > 0x10ffffU ||
        (codepoint >= 0xd800U && codepoint <= 0xdfffU)) {
      return false;
    }
  }
  return true;
}

void validate_limit(std::size_t max_payload) {
  if (max_payload == 0 || max_payload > kAbsoluteMaxPayload) {
    throw std::invalid_argument("max payload must be in 1..=64MiB");
  }
}

void append_varint(Bytes& output, std::uint64_t value) {
  while (value >= 0x80U) {
    output.push_back(static_cast<std::uint8_t>(value) | 0x80U);
    value >>= 7U;
  }
  output.push_back(static_cast<std::uint8_t>(value));
}

std::uint64_t read_varint(std::span<const std::uint8_t> bytes, std::size_t& offset) {
  std::uint64_t value = 0;
  for (unsigned shift = 0; shift < 64 && offset < bytes.size(); shift += 7) {
    const auto byte = bytes[offset++];
    value |= static_cast<std::uint64_t>(byte & 0x7fU) << shift;
    if ((byte & 0x80U) == 0) return value;
  }
  throw Elr2ProtocolError("invalid Session Control protobuf varint");
}

void skip_field(
    std::span<const std::uint8_t> bytes, std::size_t& offset, std::uint64_t wire_type) {
  switch (wire_type) {
    case 0: (void)read_varint(bytes, offset); return;
    case 1:
      if (bytes.size() - offset < 8) throw Elr2ProtocolError("truncated Session Control field");
      offset += 8;
      return;
    case 2: {
      const auto length = read_varint(bytes, offset);
      if (length > bytes.size() - offset) throw Elr2ProtocolError("truncated Session Control field");
      offset += static_cast<std::size_t>(length);
      return;
    }
    case 5:
      if (bytes.size() - offset < 4) throw Elr2ProtocolError("truncated Session Control field");
      offset += 4;
      return;
    default: throw Elr2ProtocolError("unsupported Session Control protobuf wire type");
  }
}

SessionControlAction parse_action(std::uint64_t value) {
  if (value < 1 || value > 5) throw Elr2ProtocolError("unknown Session Control action");
  return static_cast<SessionControlAction>(value);
}

}  // namespace

Bytes to_bytes(std::string_view text) {
  return {text.begin(), text.end()};
}

std::string_view as_string(std::span<const std::uint8_t> bytes) noexcept {
  if (bytes.empty()) return {};
  return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

Elr2Frame Elr2Frame::request(
    std::uint32_t route, std::uint64_t request_id, Bytes payload, std::uint32_t sequence) {
  Elr2Frame frame{FrameKind::Request, 0, route, request_id, sequence, std::move(payload)};
  Elr2Codec::validate(frame, kAbsoluteMaxPayload);
  return frame;
}

Elr2Frame Elr2Frame::response(const Elr2Frame& request, Bytes payload) {
  if (request.kind != FrameKind::Request) {
    throw Elr2ProtocolError("response source must be a request frame");
  }
  Elr2Frame frame{
      FrameKind::Response, 0, request.route, request.request_id, request.sequence,
      std::move(payload)};
  Elr2Codec::validate(frame, kAbsoluteMaxPayload);
  return frame;
}

Elr2Frame Elr2Frame::error(const Elr2Frame& request, Bytes payload) {
  if (request.kind != FrameKind::Request) {
    throw Elr2ProtocolError("error source must be a request frame");
  }
  Elr2Frame frame{
      FrameKind::Error, 0, request.route, request.request_id, request.sequence,
      std::move(payload)};
  Elr2Codec::validate(frame, kAbsoluteMaxPayload);
  return frame;
}

Elr2Frame Elr2Frame::push(std::uint32_t route, Bytes payload, std::uint32_t sequence) {
  Elr2Frame frame{FrameKind::Push, 0, route, 0, sequence, std::move(payload)};
  Elr2Codec::validate(frame, kAbsoluteMaxPayload);
  return frame;
}

void Elr2Codec::validate(const Elr2Frame& frame, std::size_t max_payload) {
  validate_limit(max_payload);
  if (frame.payload.size() > max_payload) throw Elr2ProtocolError("payload is too large");
  if (frame.flags != 0) throw Elr2ProtocolError("unsupported frame flags");
  if (frame.route == 0) throw Elr2ProtocolError("route must be non-zero");
  if (!valid_kind(frame.kind)) throw Elr2ProtocolError("unknown frame kind");
  if (frame.kind == FrameKind::Push) {
    if (frame.request_id != 0) throw Elr2ProtocolError("push request id must be zero");
  } else if (frame.request_id == 0) {
    throw Elr2ProtocolError("request id must be non-zero");
  }
}

Bytes Elr2Codec::encode(const Elr2Frame& frame, std::size_t max_payload) {
  Elr2Codec::validate(frame, max_payload);
  Bytes output(kElr2HeaderLength + frame.payload.size());
  write_u32(output, 0, kElr2Magic);
  write_u16(output, 4, kElr2Version);
  output[6] = static_cast<std::uint8_t>(frame.kind);
  output[7] = frame.flags;
  write_u32(output, 8, frame.route);
  write_u64(output, 12, frame.request_id);
  write_u32(output, 20, frame.sequence);
  write_u32(output, 24, static_cast<std::uint32_t>(frame.payload.size()));
  std::copy(frame.payload.begin(), frame.payload.end(), output.begin() + kElr2HeaderLength);
  return output;
}

Elr2Frame Elr2Codec::decode(std::span<const std::uint8_t> bytes, std::size_t max_payload) {
  validate_limit(max_payload);
  if (bytes.size() < kElr2HeaderLength) throw Elr2ProtocolError("incomplete Elura frame");
  if (read_u32(bytes) != kElr2Magic) throw Elr2ProtocolError("invalid Elura magic");
  if (read_u16(bytes.subspan(4)) != kElr2Version) {
    throw Elr2ProtocolError("unsupported Elura version");
  }
  const auto payload_size = static_cast<std::size_t>(read_u32(bytes.subspan(24)));
  if (payload_size > max_payload) throw Elr2ProtocolError("Elura payload is too large");
  if (payload_size > std::numeric_limits<std::size_t>::max() - kElr2HeaderLength ||
      bytes.size() != kElr2HeaderLength + payload_size) {
    throw Elr2ProtocolError("Elura message must contain exactly one frame");
  }
  Elr2Frame frame;
  frame.kind = parse_kind(bytes[6]);
  frame.flags = bytes[7];
  frame.route = read_u32(bytes.subspan(8));
  frame.request_id = read_u64(bytes.subspan(12));
  frame.sequence = read_u32(bytes.subspan(20));
  frame.payload.assign(bytes.begin() + kElr2HeaderLength, bytes.end());
  Elr2Codec::validate(frame, max_payload);
  return frame;
}

Elr2StreamDecoder::Elr2StreamDecoder(std::size_t max_payload) : max_payload_(max_payload) {
  validate_limit(max_payload_);
}

void Elr2StreamDecoder::append(std::span<const std::uint8_t> bytes) {
  buffer_.insert(buffer_.end(), bytes.begin(), bytes.end());
}

std::optional<Elr2Frame> Elr2StreamDecoder::next() {
  if (buffer_.size() < kElr2HeaderLength) return std::nullopt;
  const std::span<const std::uint8_t> bytes = buffer_;
  if (read_u32(bytes) != kElr2Magic) throw Elr2ProtocolError("invalid Elura magic");
  if (read_u16(bytes.subspan(4)) != kElr2Version) {
    throw Elr2ProtocolError("unsupported Elura version");
  }
  const auto payload_size = static_cast<std::size_t>(read_u32(bytes.subspan(24)));
  if (payload_size > max_payload_) throw Elr2ProtocolError("Elura payload is too large");
  const auto total = kElr2HeaderLength + payload_size;
  if (buffer_.size() < total) return std::nullopt;
  auto frame = Elr2Codec::decode(bytes.first(total), max_payload_);
  buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(total));
  return frame;
}

Bytes encode_session_control(const SessionControl& control) {
  const auto action = static_cast<std::int32_t>(control.action);
  if (action < 1 || action > 5) throw Elr2ProtocolError("unknown Session Control action");
  if (control.reason.size() > 256) throw Elr2ProtocolError("Session Control reason exceeds 256 bytes");
  if (!valid_utf8(control.reason)) throw Elr2ProtocolError("invalid Session Control UTF-8");
  Bytes output{0x08U};
  append_varint(output, static_cast<std::uint64_t>(action));
  if (!control.reason.empty()) {
    output.push_back(0x12U);
    append_varint(output, control.reason.size());
    output.insert(output.end(), control.reason.begin(), control.reason.end());
  }
  return output;
}

SessionControl decode_session_control(std::span<const std::uint8_t> bytes) {
  std::size_t offset = 0;
  std::uint64_t action = 0;
  std::string reason;
  while (offset < bytes.size()) {
    const auto tag = read_varint(bytes, offset);
    const auto field = tag >> 3U;
    const auto wire_type = tag & 7U;
    if (field == 0) throw Elr2ProtocolError("invalid Session Control protobuf tag");
    if (field == 1 && wire_type == 0) {
      action = read_varint(bytes, offset);
    } else if (field == 2 && wire_type == 2) {
      const auto length = read_varint(bytes, offset);
      if (length > bytes.size() - offset) throw Elr2ProtocolError("truncated Session Control reason");
      reason.assign(
          reinterpret_cast<const char*>(bytes.data() + offset),
          static_cast<std::size_t>(length));
      offset += static_cast<std::size_t>(length);
    } else {
      skip_field(bytes, offset, wire_type);
    }
  }
  if (reason.size() > 256) throw Elr2ProtocolError("Session Control reason exceeds 256 bytes");
  if (!valid_utf8(reason)) throw Elr2ProtocolError("invalid Session Control UTF-8");
  return SessionControl{parse_action(action), std::move(reason)};
}

void validate_identity(const Identity& identity) {
  if (identity.account_id <= 0 || identity.user_id <= 0 || identity.region_id == 0 ||
      identity.realm_id == 0 || identity.generation == 0) {
    throw Elr2ProtocolError("invalid identity");
  }
}

void validate_error_envelope(const ErrorEnvelope& envelope) {
  if (envelope.code.empty() || envelope.code.size() > 64 || envelope.message.size() > 1024 ||
      (envelope.retry_after_ms.has_value() && *envelope.retry_after_ms == 0) ||
      !valid_utf8(envelope.message) ||
      !std::all_of(envelope.code.begin(), envelope.code.end(), [](unsigned char value) {
        return (value >= 'A' && value <= 'Z') || (value >= '0' && value <= '9') || value == '_';
      })) {
    throw Elr2ProtocolError("invalid error envelope fields");
  }
}

void EluraProtocol::validate_client_frame(
    const Elr2Frame& frame, bool authenticated, std::optional<std::uint64_t> pending_heartbeat) {
  Elr2Codec::validate(frame);
  if (frame.kind == FrameKind::Response && frame.route == EluraRoutes::Heartbeat) {
    if (!pending_heartbeat || frame.request_id != *pending_heartbeat || frame.sequence != 0 ||
        !frame.payload.empty()) {
      throw Elr2ProtocolError("heartbeat response does not match an outstanding request");
    }
    return;
  }
  if (frame.kind != FrameKind::Request) {
    throw Elr2ProtocolError(
        "the Elura endpoint accepts request frames and heartbeat responses only");
  }
  if (frame.route < EluraRoutes::FirstApplication && frame.sequence != 0) {
    throw Elr2ProtocolError("framework requests must have sequence zero");
  }
  const bool allowed = authenticated
      ? frame.route == EluraRoutes::Heartbeat || frame.route == EluraRoutes::RenewReconnectTicket ||
            frame.route >= EluraRoutes::FirstApplication
      : frame.route == EluraRoutes::Authenticate;
  if (!allowed) throw Elr2ProtocolError("route is not allowed in the current session state");
}

Elr2Frame EluraProtocol::heartbeat_response(const Elr2Frame& request) {
  if (request.kind != FrameKind::Request || request.route != EluraRoutes::Heartbeat ||
      request.sequence != 0 || !request.payload.empty()) {
    throw Elr2ProtocolError("invalid heartbeat request");
  }
  return Elr2Frame{FrameKind::Response, 0, request.route, request.request_id, 0, {}};
}

}  // namespace elura
