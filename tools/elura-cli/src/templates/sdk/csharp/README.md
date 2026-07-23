# Elura protocol for Unity and C#

`Elura.Protocol` is a dependency-free C# 9 / .NET Standard 2.1 implementation of the public
Elura client ELR2 v{{ELR2_VERSION}} contract. It is compatible with Unity 6.3 LTS and its
Mono and IL2CPP scripting backends.

It includes frame encoding, TCP stream reassembly, authentication and reconnect payloads,
standard error envelopes, heartbeat handling, and Session Control protobuf encoding. Socket
creation and application-route dispatch remain in your game code.

## Unity installation

Either copy the two files under `Elura.Protocol/` into your Unity project's `Assets` directory,
or build the managed plug-in and copy the DLL from
`Elura.Protocol/bin/Release/netstandard2.1/` into `Assets/Plugins/Elura/`:

```sh
dotnet build Elura.Protocol/Elura.Protocol.csproj -c Release
```

## Quick start

Authentication is one line; the SDK creates both the JSON payload and ELR2 frame:

```csharp
var request = EluraProtocol.Authenticate(nextRequestId++, loginTicket);
var bytes = Elr2Codec.Encode(request);
socket.Send(bytes);
```

Application routes start at 100:

```csharp
var request = Elr2Frame.Request(100, nextRequestId++, Elr2.Utf8(json));
socket.Send(Elr2Codec.Encode(request));
```

A WebSocket binary message contains exactly one frame:

```csharp
var frame = Elr2Codec.Decode(message);
```

TCP and QUIC streams can split or combine frames, so append every received chunk and read until
the decoder needs more bytes:

```csharp
decoder.Append(chunk);
while (decoder.TryRead(out var frame))
{
    Handle(frame!);
}
```

Reply to a server heartbeat with:

```csharp
socket.Send(Elr2Codec.Encode(EluraProtocol.HeartbeatResponse(heartbeat)));
```

For WebSocket transport, negotiate `{{PROTOCOL_IDENTIFIER}}` as the subprotocol. Every successful
authentication response contains a reconnect ticket. Retain only the latest ticket, renew it
before `ExpiresInSeconds`, and replace it with the ticket returned by the renewal response.

Run the golden-vector executable outside Unity:

```sh
dotnet run --project Elura.Protocol.Tests/Elura.Protocol.Tests.csproj
```
