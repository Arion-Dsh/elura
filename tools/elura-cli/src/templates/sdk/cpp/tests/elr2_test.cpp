#include "elura/elr2.hpp"

#include <cassert>
#include <string>
#include <vector>

int main() {
  elura::Frame request;
  request.kind = elura::FrameKind::Request;
  request.route = 100;
  request.request_id = 7;
  request.sequence = 11;
  request.payload = {'h', 'e', 'l', 'l', 'o'};
  const std::vector<std::uint8_t> expected = {
      0x45, 0x4c, 0x52, 0x32, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00,
      0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00,
      0x00, 0x0b, 0x00, 0x00, 0x00, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f};
  const auto encoded = elura::encode_frame(request);
  assert(encoded == expected);
  const auto decoded = elura::decode_frame(encoded);
  assert(decoded.route == 100 && decoded.request_id == 7 && decoded.payload == request.payload);

  elura::StreamDecoder stream;
  stream.append(encoded.data(), 10);
  elura::Frame streamed;
  assert(!stream.next(streamed));
  stream.append(encoded.data() + 10, encoded.size() - 10);
  assert(stream.next(streamed));
  assert(streamed.payload == request.payload && stream.buffered() == 0);

  auto wrong_version = encoded;
  wrong_version[5] = 3;
  bool rejected_wrong_version = false;
  try {
    (void)elura::decode_frame(wrong_version);
  } catch (const elura::ProtocolError&) {
    rejected_wrong_version = true;
  }
  assert(rejected_wrong_version);

  const elura::SessionControl control{
      elura::SessionControlAction::AccountVersionChanged, "credentials rotated"};
  const auto control_bytes = elura::encode_session_control(control);
  const std::vector<std::uint8_t> expected_control = {
      0x08, 0x02, 0x12, 0x13, 'c', 'r', 'e', 'd', 'e', 'n', 't', 'i', 'a', 'l', 's',
      ' ',  'r',  'o',  't',  'a',  't',  'e',  'd'};
  assert(control_bytes == expected_control);
  const auto decoded_control = elura::decode_session_control(control_bytes);
  assert(decoded_control.action == control.action && decoded_control.reason == control.reason);
  return 0;
}
