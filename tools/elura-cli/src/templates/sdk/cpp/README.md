# Elura Gateway protocol for C++

This dependency-free C++17 library implements the public Gateway-to-client ELR2 v{{ELR2_VERSION}}
wire contract. It contains frame encoding, exact-message decoding, TCP stream reassembly,
reserved routes, JSON payload model types, and Session Control protobuf encoding.

Build and run the golden-vector tests:

```sh
cmake -S . -B build -DBUILD_TESTING=ON
cmake --build build
ctest --test-dir build --output-on-failure
```

Transport integration is intentionally outside this package:

- TCP and QUIC streams carry consecutive encoded frames.
- Each WebSocket binary message contains exactly one encoded frame and negotiates
  `{{PROTOCOL_IDENTIFIER}}` as its subprotocol.
- Authentication, reconnect, responses, and errors use the JSON field names documented by the
  model structs in `elr2.hpp`. Use the JSON library already selected by your client application.
- `proto/session_control.proto` is included for projects that prefer generated protobuf types.
