using System.Text;
using Elura.Protocol;

var request = new Elr2Frame(FrameKind.Request, 0, 100, 7, 11, "hello"u8.ToArray());
byte[] expected =
[
    0x45, 0x4c, 0x52, 0x32, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00,
    0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00,
    0x00, 0x0b, 0x00, 0x00, 0x00, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f,
];
var encoded = Elr2Codec.Encode(request);
Assert(encoded.SequenceEqual(expected), "ELR2 v2 golden vector");
var decoded = Elr2Codec.Decode(encoded);
Assert(decoded.Route == 100 && decoded.RequestId == 7 && decoded.Payload.SequenceEqual(request.Payload),
    "frame round trip");

var stream = new Elr2StreamDecoder();
stream.Append(encoded.AsSpan(0, 10));
Assert(!stream.TryRead(out _), "partial stream frame");
stream.Append(encoded.AsSpan(10));
Assert(stream.TryRead(out var streamed) && streamed!.Payload.SequenceEqual(request.Payload),
    "stream frame completion");

var wrongVersion = (byte[])encoded.Clone();
wrongVersion[5] = 3;
AssertThrows(() => Elr2Codec.Decode(wrongVersion), "wire-version mismatch");

var auth = GatewayPayloadCodec.EncodeAuthenticateRequest("ticket-value");
Assert(Encoding.UTF8.GetString(auth) == "{\"ticket\":\"ticket-value\"}", "authentication JSON");

var control = new SessionControl(SessionControlAction.AccountVersionChanged, "credentials rotated");
var controlBytes = SessionControlCodec.Encode(control);
byte[] expectedControl = [0x08, 0x02, 0x12, 0x13, .. "credentials rotated"u8.ToArray()];
Assert(controlBytes.SequenceEqual(expectedControl), "Session Control golden vector");
Assert(SessionControlCodec.Decode(controlBytes) == control, "Session Control protobuf");

Console.WriteLine("Elura.Protocol golden vectors passed.");

static void Assert(bool condition, string name)
{
    if (!condition)
        throw new Exception($"assertion failed: {name}");
}

static void AssertThrows(Action action, string name)
{
    try
    {
        action();
    }
    catch (Elr2ProtocolException)
    {
        return;
    }
    throw new Exception($"assertion failed: {name}");
}
