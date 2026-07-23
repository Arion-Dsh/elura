#include "elura/elr2.hpp"

#include <cassert>

int main() {
  const auto request = elura::Elr2Frame::request(100, 7, elura::to_bytes("hello"), 11);
  const elura::Bytes expected = {
      0x45, 0x4c, 0x52, 0x32, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00,
      0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00,
      0x00, 0x0b, 0x00, 0x00, 0x00, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f};
  const auto encoded = elura::Elr2Codec::encode(request);
  assert(encoded == expected);
  assert(elura::Elr2Codec::decode(encoded) == request);
  assert(elura::as_string(request.payload) == "hello");

  elura::Elr2StreamDecoder stream;
  stream.append(std::span{encoded}.first(10));
  assert(!stream.next());
  stream.append(std::span{encoded}.subspan(10));
  const auto streamed = stream.next();
  assert(streamed == request && stream.empty());

  const auto response = elura::Elr2Frame::response(request, elura::to_bytes("ok"));
  assert(response.kind == elura::FrameKind::Response);
  assert(response.route == request.route && response.request_id == request.request_id);
  const auto push = elura::Elr2Frame::push(101, elura::to_bytes("news"));
  assert(push.kind == elura::FrameKind::Push && push.request_id == 0);

  auto wrong_version = encoded;
  wrong_version[5] = 3;
  bool rejected_wrong_version = false;
  try {
    (void)elura::Elr2Codec::decode(wrong_version);
  } catch (const elura::Elr2ProtocolError&) {
    rejected_wrong_version = true;
  }
  assert(rejected_wrong_version);

  const elura::SessionControl control{
      elura::SessionControlAction::AccountVersionChanged, "credentials rotated"};
  const auto control_bytes = elura::encode_session_control(control);
  const elura::Bytes expected_control = {
      0x08, 0x02, 0x12, 0x13, 'c', 'r', 'e', 'd', 'e', 'n', 't', 'i', 'a', 'l', 's',
      ' ',  'r',  'o',  't',  'a',  't',  'e',  'd'};
  assert(control_bytes == expected_control);
  const auto decoded_control = elura::decode_session_control(control_bytes);
  assert(decoded_control.action == control.action && decoded_control.reason == control.reason);
  return 0;
}
