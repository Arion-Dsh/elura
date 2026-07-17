#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace elura {

inline constexpr std::uint32_t kElr2Magic = 0x454c5232U;
inline constexpr std::uint16_t kElr2Version = {{ELR2_VERSION}};
inline constexpr std::size_t kElr2HeaderLength = 28;
inline constexpr std::size_t kDefaultMaxPayload = 1U << 20U;
inline constexpr std::size_t kAbsoluteMaxPayload = 64U << 20U;
inline constexpr const char* kProtocolIdentifier = "{{PROTOCOL_IDENTIFIER}}";

inline constexpr std::uint32_t kRouteAuthenticate = 1;
inline constexpr std::uint32_t kRouteHeartbeat = 2;
inline constexpr std::uint32_t kRouteReconnect = 3;
inline constexpr std::uint32_t kRouteSessionControl = 4;
inline constexpr std::uint32_t kFirstApplicationRoute = 100;

enum class FrameKind : std::uint8_t {
  Request = 1,
  Response = 2,
  Push = 3,
  Error = 4,
};

struct Frame {
  FrameKind kind = FrameKind::Request;
  std::uint8_t flags = 0;
  std::uint32_t route = 0;
  std::uint64_t request_id = 0;
  std::uint32_t sequence = 0;
  std::vector<std::uint8_t> payload;
};

class ProtocolError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

void validate_frame(const Frame& frame, std::size_t max_payload = kDefaultMaxPayload);
std::vector<std::uint8_t> encode_frame(
    const Frame& frame, std::size_t max_payload = kDefaultMaxPayload);
Frame decode_frame(
    const std::uint8_t* bytes, std::size_t size,
    std::size_t max_payload = kDefaultMaxPayload);
inline Frame decode_frame(
    const std::vector<std::uint8_t>& bytes,
    std::size_t max_payload = kDefaultMaxPayload) {
  return decode_frame(bytes.data(), bytes.size(), max_payload);
}

class StreamDecoder {
 public:
  explicit StreamDecoder(std::size_t max_payload = kDefaultMaxPayload);
  void append(const std::uint8_t* bytes, std::size_t size);
  void append(const std::vector<std::uint8_t>& bytes) { append(bytes.data(), bytes.size()); }
  bool next(Frame& frame);
  std::size_t buffered() const noexcept { return buffer_.size(); }

 private:
  std::size_t max_payload_;
  std::vector<std::uint8_t> buffer_;
};

// JSON payload models. Serialized field names are the snake_case names shown below.
struct Identity {
  std::int64_t account_id = 0;
  std::int64_t user_id = 0;
  std::uint32_t region_id = 0;
  std::uint32_t realm_id = 0;
  std::uint64_t generation = 0;
};

struct AuthenticateRequest { std::string ticket; };
struct ReconnectTicketRequest { std::string ticket; };
struct ReconnectTicketResponse {
  std::string ticket;
  std::uint64_t expires_in_seconds = 0;
};
struct AuthenticateResponse {
  std::string session_id;
  Identity identity;
  ReconnectTicketResponse reconnect;
};
struct ErrorEnvelope {
  std::string code;
  std::string message;
  bool retryable = false;
  std::optional<std::uint64_t> retry_after_ms;
};

void validate_identity(const Identity& identity);
void validate_error_envelope(const ErrorEnvelope& envelope);
void validate_client_frame(
    const Frame& frame, bool authenticated,
    std::optional<std::uint64_t> pending_heartbeat = std::nullopt);
Frame heartbeat_response(const Frame& request);

enum class SessionControlAction : std::int32_t {
  Kick = 1,
  AccountVersionChanged = 2,
  DuplicateLogin = 3,
  ForceLogout = 4,
  ServerDraining = 5,
};

struct SessionControl {
  SessionControlAction action = SessionControlAction::Kick;
  std::string reason;
};

std::vector<std::uint8_t> encode_session_control(const SessionControl& control);
SessionControl decode_session_control(const std::uint8_t* bytes, std::size_t size);
inline SessionControl decode_session_control(const std::vector<std::uint8_t>& bytes) {
  return decode_session_control(bytes.data(), bytes.size());
}

}  // namespace elura
