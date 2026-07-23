# Elura Gateway protocol for C++

This dependency-free C++20 library implements the public Gateway-to-client ELR2 v{{ELR2_VERSION}}
wire contract. It contains frame encoding, exact-message decoding, TCP stream reassembly,
reserved routes, JSON payload model types, and Session Control protobuf encoding.

## Quick start

```cpp
#include <elura/elr2.hpp>

auto request = elura::Frame::request(
    100, request_id, elura::to_bytes(R"({"name":"Ada"})"));
auto wire_bytes = elura::encode_frame(request);

// A complete WebSocket message.
auto response = elura::decode_frame(received_bytes);

// A TCP or QUIC stream. std::vector, std::array, and std::span work directly.
elura::StreamDecoder decoder;
decoder.append(received_chunk);
while (auto frame = decoder.next()) {
  handle(*frame);
}
```

Build and run the golden-vector tests:

```sh
cmake -S . -B build -DBUILD_TESTING=ON
cmake --build build
ctest --test-dir build --output-on-failure
```

When embedding the SDK with `add_subdirectory`, link the namespaced target:

```cmake
target_link_libraries(my_client PRIVATE elura::protocol)
```

Transport integration is intentionally outside this package:

- TCP and QUIC streams carry consecutive encoded frames.
- Each WebSocket binary message contains exactly one encoded frame and negotiates
  `{{PROTOCOL_IDENTIFIER}}` as its subprotocol.
- Authentication, reconnect, responses, and errors use the JSON field names documented by the
  model structs in `elr2.hpp`. Use the JSON library already selected by your client application.
- `proto/session_control.proto` is included for projects that prefer generated protobuf types.

Every successful authentication response contains a reconnect ticket. Retain only the latest
ticket, renew it before `expires_in_seconds` through the reconnect route, and replace it with the
ticket returned by that response. The renewal request consumes the previous ticket.
