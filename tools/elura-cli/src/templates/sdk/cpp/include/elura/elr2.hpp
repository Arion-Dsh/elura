#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace elura {

inline constexpr std::uint32_t kElr2Magic = 0x454c5232U;
inline constexpr std::uint16_t kElr2Version = {{ELR2_VERSION}};
inline constexpr std::size_t kElr2HeaderLength = 28;
inline constexpr std::size_t kDefaultMaxPayload = 1U << 20U;
inline constexpr std::size_t kAbsoluteMaxPayload = 64U << 20U;
inline constexpr const char* kProtocolIdentifier = "{{PROTOCOL_IDENTIFIER}}";

struct EluraRoutes {
  static constexpr std::uint32_t Authenticate = 1;
  static constexpr std::uint32_t Heartbeat = 2;
  static constexpr std::uint32_t RenewReconnectTicket = 3;
  static constexpr std::uint32_t SessionControl = 4;
  static constexpr std::uint32_t FirstApplication = 100;
};

enum class FrameKind : std::uint8_t {
  Request = 1,
  Response = 2,
  Push = 3,
  Error = 4,
};

using Bytes = std::vector<std::uint8_t>;

[[nodiscard]] Bytes to_bytes(std::string_view text);
[[nodiscard]] std::string_view as_string(std::span<const std::uint8_t> bytes) noexcept;

struct Elr2Frame {
  FrameKind kind = FrameKind::Request;
  std::uint8_t flags = 0;
  std::uint32_t route = 0;
  std::uint64_t request_id = 0;
  std::uint32_t sequence = 0;
  Bytes payload;

  [[nodiscard]] static Elr2Frame request(
      std::uint32_t route, std::uint64_t request_id, Bytes payload = {},
      std::uint32_t sequence = 0);
  [[nodiscard]] static Elr2Frame response(const Elr2Frame& request, Bytes payload = {});
  [[nodiscard]] static Elr2Frame error(const Elr2Frame& request, Bytes payload = {});
  [[nodiscard]] static Elr2Frame push(
      std::uint32_t route, Bytes payload = {}, std::uint32_t sequence = 0);

  bool operator==(const Elr2Frame&) const = default;
};

class Elr2ProtocolError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

class Elr2Codec {
 public:
  static void validate(
      const Elr2Frame& frame, std::size_t max_payload = kDefaultMaxPayload);
  [[nodiscard]] static Bytes encode(
      const Elr2Frame& frame, std::size_t max_payload = kDefaultMaxPayload);
  [[nodiscard]] static Elr2Frame decode(
      std::span<const std::uint8_t> bytes,
      std::size_t max_payload = kDefaultMaxPayload);
};

class Elr2StreamDecoder {
 public:
  explicit Elr2StreamDecoder(std::size_t max_payload = kDefaultMaxPayload);
  void append(std::span<const std::uint8_t> bytes);
  [[nodiscard]] std::optional<Elr2Frame> next();
  [[nodiscard]] std::size_t buffered() const noexcept { return buffer_.size(); }
  [[nodiscard]] bool empty() const noexcept { return buffer_.empty(); }

 private:
  std::size_t max_payload_;
  Bytes buffer_;
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
struct ReconnectTicketRenewalRequest { std::string ticket; };
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
class EluraProtocol {
 public:
  static void validate_client_frame(
      const Elr2Frame& frame, bool authenticated,
      std::optional<std::uint64_t> pending_heartbeat = std::nullopt);
  [[nodiscard]] static Elr2Frame heartbeat_response(const Elr2Frame& request);
};

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

[[nodiscard]] Bytes encode_session_control(const SessionControl& control);
[[nodiscard]] SessionControl decode_session_control(std::span<const std::uint8_t> bytes);

}  // namespace elura
