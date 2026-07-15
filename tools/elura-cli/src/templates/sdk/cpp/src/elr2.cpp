#include "elura/elr2.hpp"

#include <algorithm>
#include <limits>

namespace elura {
namespace {

std::uint16_t read_u16(const std::uint8_t* bytes) {
  return static_cast<std::uint16_t>((static_cast<std::uint16_t>(bytes[0]) << 8U) |
                                    static_cast<std::uint16_t>(bytes[1]));
}

std::uint32_t read_u32(const std::uint8_t* bytes) {
  return (static_cast<std::uint32_t>(bytes[0]) << 24U) |
         (static_cast<std::uint32_t>(bytes[1]) << 16U) |
         (static_cast<std::uint32_t>(bytes[2]) << 8U) |
         static_cast<std::uint32_t>(bytes[3]);
}

std::uint64_t read_u64(const std::uint8_t* bytes) {
  return (static_cast<std::uint64_t>(read_u32(bytes)) << 32U) | read_u32(bytes + 4);
}

void write_u16(std::vector<std::uint8_t>& output, std::size_t offset, std::uint16_t value) {
  output[offset] = static_cast<std::uint8_t>(value >> 8U);
  output[offset + 1] = static_cast<std::uint8_t>(value);
}

void write_u32(std::vector<std::uint8_t>& output, std::size_t offset, std::uint32_t value) {
  output[offset] = static_cast<std::uint8_t>(value >> 24U);
  output[offset + 1] = static_cast<std::uint8_t>(value >> 16U);
  output[offset + 2] = static_cast<std::uint8_t>(value >> 8U);
  output[offset + 3] = static_cast<std::uint8_t>(value);
}

void write_u64(std::vector<std::uint8_t>& output, std::size_t offset, std::uint64_t value) {
  write_u32(output, offset, static_cast<std::uint32_t>(value >> 32U));
  write_u32(output, offset + 4, static_cast<std::uint32_t>(value));
}

FrameKind parse_kind(std::uint8_t value) {
  switch (value) {
    case 1: return FrameKind::Request;
    case 2: return FrameKind::Response;
    case 3: return FrameKind::Push;
    case 4: return FrameKind::Error;
    default: throw ProtocolError("unknown frame kind");
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

void append_varint(std::vector<std::uint8_t>& output, std::uint64_t value) {
  while (value >= 0x80U) {
    output.push_back(static_cast<std::uint8_t>(value) | 0x80U);
    value >>= 7U;
  }
  output.push_back(static_cast<std::uint8_t>(value));
}

std::uint64_t read_varint(const std::uint8_t* bytes, std::size_t size, std::size_t& offset) {
  std::uint64_t value = 0;
  for (unsigned shift = 0; shift < 64 && offset < size; shift += 7) {
    const auto byte = bytes[offset++];
    value |= static_cast<std::uint64_t>(byte & 0x7fU) << shift;
    if ((byte & 0x80U) == 0) return value;
  }
  throw ProtocolError("invalid Session Control protobuf varint");
}

void skip_field(
    const std::uint8_t* bytes, std::size_t size, std::size_t& offset, std::uint64_t wire_type) {
  switch (wire_type) {
    case 0: (void)read_varint(bytes, size, offset); return;
    case 1:
      if (size - offset < 8) throw ProtocolError("truncated Session Control field");
      offset += 8;
      return;
    case 2: {
      const auto length = read_varint(bytes, size, offset);
      if (length > size - offset) throw ProtocolError("truncated Session Control field");
      offset += static_cast<std::size_t>(length);
      return;
    }
    case 5:
      if (size - offset < 4) throw ProtocolError("truncated Session Control field");
      offset += 4;
      return;
    default: throw ProtocolError("unsupported Session Control protobuf wire type");
  }
}

SessionControlAction parse_action(std::uint64_t value) {
  if (value < 1 || value > 5) throw ProtocolError("unknown Session Control action");
  return static_cast<SessionControlAction>(value);
}

}  // namespace

void validate_frame(const Frame& frame, std::size_t max_payload) {
  validate_limit(max_payload);
  if (frame.payload.size() > max_payload) throw ProtocolError("payload is too large");
  if (frame.flags != 0) throw ProtocolError("unsupported frame flags");
  if (frame.route == 0) throw ProtocolError("route must be non-zero");
  if (!valid_kind(frame.kind)) throw ProtocolError("unknown frame kind");
  if (frame.kind == FrameKind::Push) {
    if (frame.request_id != 0) throw ProtocolError("push request id must be zero");
  } else if (frame.request_id == 0) {
    throw ProtocolError("request id must be non-zero");
  }
}

std::vector<std::uint8_t> encode_frame(const Frame& frame, std::size_t max_payload) {
  validate_frame(frame, max_payload);
  std::vector<std::uint8_t> output(kElr2HeaderLength + frame.payload.size());
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

Frame decode_frame(const std::uint8_t* bytes, std::size_t size, std::size_t max_payload) {
  validate_limit(max_payload);
  if (size < kElr2HeaderLength) throw ProtocolError("incomplete Elura frame");
  if (read_u32(bytes) != kElr2Magic) throw ProtocolError("invalid Elura magic");
  if (read_u16(bytes + 4) != kElr2Version) throw ProtocolError("unsupported Elura version");
  const auto payload_size = static_cast<std::size_t>(read_u32(bytes + 24));
  if (payload_size > max_payload) throw ProtocolError("Elura payload is too large");
  if (payload_size > std::numeric_limits<std::size_t>::max() - kElr2HeaderLength ||
      size != kElr2HeaderLength + payload_size) {
    throw ProtocolError("Elura message must contain exactly one frame");
  }
  Frame frame;
  frame.kind = parse_kind(bytes[6]);
  frame.flags = bytes[7];
  frame.route = read_u32(bytes + 8);
  frame.request_id = read_u64(bytes + 12);
  frame.sequence = read_u32(bytes + 20);
  frame.payload.assign(bytes + kElr2HeaderLength, bytes + size);
  validate_frame(frame, max_payload);
  return frame;
}

StreamDecoder::StreamDecoder(std::size_t max_payload) : max_payload_(max_payload) {
  validate_limit(max_payload_);
}

void StreamDecoder::append(const std::uint8_t* bytes, std::size_t size) {
  if (size != 0 && bytes == nullptr) throw std::invalid_argument("bytes must not be null");
  if (size == 0) return;
  buffer_.insert(buffer_.end(), bytes, bytes + size);
}

bool StreamDecoder::next(Frame& frame) {
  if (buffer_.size() < kElr2HeaderLength) return false;
  if (read_u32(buffer_.data()) != kElr2Magic) throw ProtocolError("invalid Elura magic");
  if (read_u16(buffer_.data() + 4) != kElr2Version) {
    throw ProtocolError("unsupported Elura version");
  }
  const auto payload_size = static_cast<std::size_t>(read_u32(buffer_.data() + 24));
  if (payload_size > max_payload_) throw ProtocolError("Elura payload is too large");
  const auto total = kElr2HeaderLength + payload_size;
  if (buffer_.size() < total) return false;
  frame = decode_frame(buffer_.data(), total, max_payload_);
  buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(total));
  return true;
}

std::vector<std::uint8_t> encode_session_control(const SessionControl& control) {
  const auto action = static_cast<std::int32_t>(control.action);
  if (action < 1 || action > 5) throw ProtocolError("unknown Session Control action");
  if (control.reason.size() > 256) throw ProtocolError("Session Control reason exceeds 256 bytes");
  if (!valid_utf8(control.reason)) throw ProtocolError("invalid Session Control UTF-8");
  std::vector<std::uint8_t> output{0x08U};
  append_varint(output, static_cast<std::uint64_t>(action));
  if (!control.reason.empty()) {
    output.push_back(0x12U);
    append_varint(output, control.reason.size());
    output.insert(output.end(), control.reason.begin(), control.reason.end());
  }
  return output;
}

SessionControl decode_session_control(const std::uint8_t* bytes, std::size_t size) {
  std::size_t offset = 0;
  std::uint64_t action = 0;
  std::string reason;
  while (offset < size) {
    const auto tag = read_varint(bytes, size, offset);
    const auto field = tag >> 3U;
    const auto wire_type = tag & 7U;
    if (field == 0) throw ProtocolError("invalid Session Control protobuf tag");
    if (field == 1 && wire_type == 0) {
      action = read_varint(bytes, size, offset);
    } else if (field == 2 && wire_type == 2) {
      const auto length = read_varint(bytes, size, offset);
      if (length > size - offset) throw ProtocolError("truncated Session Control reason");
      reason.assign(reinterpret_cast<const char*>(bytes + offset), static_cast<std::size_t>(length));
      offset += static_cast<std::size_t>(length);
    } else {
      skip_field(bytes, size, offset, wire_type);
    }
  }
  if (reason.size() > 256) throw ProtocolError("Session Control reason exceeds 256 bytes");
  if (!valid_utf8(reason)) throw ProtocolError("invalid Session Control UTF-8");
  return SessionControl{parse_action(action), std::move(reason)};
}

void validate_identity(const Identity& identity) {
  if (identity.account_id <= 0 || identity.user_id <= 0 || identity.region_id == 0 ||
      identity.realm_id == 0 || identity.generation == 0) {
    throw ProtocolError("invalid identity");
  }
}

void validate_error_envelope(const ErrorEnvelope& envelope) {
  if (envelope.code.empty() || envelope.code.size() > 64 || envelope.message.size() > 1024 ||
      !valid_utf8(envelope.message) ||
      !std::all_of(envelope.code.begin(), envelope.code.end(), [](unsigned char value) {
        return (value >= 'A' && value <= 'Z') || (value >= '0' && value <= '9') || value == '_';
      })) {
    throw ProtocolError("invalid error envelope fields");
  }
}

void validate_client_frame(
    const Frame& frame, bool authenticated, std::optional<std::uint64_t> pending_heartbeat) {
  validate_frame(frame);
  if (frame.kind == FrameKind::Response && frame.route == kRouteHeartbeat) {
    if (!pending_heartbeat || frame.request_id != *pending_heartbeat || frame.sequence != 0 ||
        !frame.payload.empty()) {
      throw ProtocolError("heartbeat response does not match an outstanding request");
    }
    return;
  }
  if (frame.kind != FrameKind::Request) {
    throw ProtocolError("Gateway accepts request frames and heartbeat responses only");
  }
  if (frame.route < kFirstApplicationRoute && frame.sequence != 0) {
    throw ProtocolError("framework requests must have sequence zero");
  }
  const bool allowed = authenticated
      ? frame.route == kRouteHeartbeat || frame.route == kRouteReconnect ||
            frame.route >= kFirstApplicationRoute
      : frame.route == kRouteAuthenticate;
  if (!allowed) throw ProtocolError("route is not allowed in the current session state");
}

Frame heartbeat_response(const Frame& request) {
  if (request.kind != FrameKind::Request || request.route != kRouteHeartbeat ||
      request.sequence != 0 || !request.payload.empty()) {
    throw ProtocolError("invalid heartbeat request");
  }
  return Frame{FrameKind::Response, 0, request.route, request.request_id, 0, {}};
}

}  // namespace elura
