using System;
using System.Linq;
using System.Text;
using Elura.Protocol;

namespace Elura.Protocol.Tests
{
    internal static class Program
    {
        private static void Main()
        {
            var request = Elr2Frame.Request(100, 7, Elr2.Utf8("hello"), 11);
            var expected = new byte[]
            {
                0x45, 0x4c, 0x52, 0x32, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00,
                0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00,
                0x00, 0x0b, 0x00, 0x00, 0x00, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f,
            };
            var encoded = Elr2Codec.Encode(request);
            Assert(encoded.SequenceEqual(expected), "ELR2 v2 golden vector");
            var decoded = Elr2Codec.Decode(encoded);
            Assert(
                decoded.Route == 100 &&
                decoded.RequestId == 7 &&
                Elr2.Text(decoded.Payload) == "hello",
                "frame round trip");

            var response = Elr2Frame.Response(request, Elr2.Utf8("ok"));
            Assert(
                response.Kind == FrameKind.Response && response.RequestId == request.RequestId,
                "response factory");
            var push = Elr2Frame.Push(101, Elr2.Utf8("news"));
            Assert(push.Kind == FrameKind.Push && push.RequestId == 0, "push factory");

            var stream = new Elr2StreamDecoder();
            stream.Append(encoded.AsSpan(0, 10));
            Elr2Frame? streamed;
            Assert(!stream.TryRead(out streamed), "partial stream frame");
            stream.Append(encoded.AsSpan(10));
            Assert(
                stream.TryRead(out streamed) &&
                streamed != null &&
                streamed.Payload.SequenceEqual(request.Payload),
                "stream frame completion");

            var wrongVersion = (byte[])encoded.Clone();
            wrongVersion[5] = 3;
            AssertThrows(() => Elr2Codec.Decode(wrongVersion), "wire-version mismatch");

            var authFrame = GatewayFrames.Authenticate(8, "ticket-value");
            Assert(
                authFrame.Route == GatewayRoutes.Authenticate &&
                Encoding.UTF8.GetString(authFrame.Payload) == "{\"ticket\":\"ticket-value\"}",
                "authentication frame");
            var reconnectFrame = GatewayFrames.Reconnect(9, "reconnect-value");
            Assert(
                reconnectFrame.Route == GatewayRoutes.Reconnect &&
                Encoding.UTF8.GetString(reconnectFrame.Payload) ==
                    "{\"ticket\":\"reconnect-value\"}",
                "reconnect renewal frame");

            var authResponse = GatewayPayloadCodec.DecodeAuthenticateResponse(
                Elr2.Utf8(
                    "{\"session_id\":\"session-1\",\"identity\":{" +
                    "\"account_id\":1,\"user_id\":2,\"region_id\":3," +
                    "\"realm_id\":4,\"generation\":5},\"reconnect\":{" +
                    "\"ticket\":\"next-ticket\",\"expires_in_seconds\":60}}"));
            Assert(
                authResponse.Identity.UserId == 2 &&
                authResponse.Reconnect.ExpiresInSeconds == 60,
                "authentication response JSON");

            var control = new SessionControl(
                SessionControlAction.AccountVersionChanged,
                "credentials rotated");
            var controlBytes = SessionControlCodec.Encode(control);
            var expectedControl = new byte[] { 0x08, 0x02, 0x12, 0x13 }
                .Concat(Encoding.UTF8.GetBytes("credentials rotated"))
                .ToArray();
            Assert(controlBytes.SequenceEqual(expectedControl), "Session Control golden vector");
            var decodedControl = SessionControlCodec.Decode(controlBytes);
            Assert(
                decodedControl.Action == control.Action && decodedControl.Reason == control.Reason,
                "Session Control protobuf");

            Console.WriteLine("Elura.Protocol golden vectors passed.");
        }

        private static void Assert(bool condition, string name)
        {
            if (!condition)
                throw new Exception("assertion failed: " + name);
        }

        private static void AssertThrows(Action action, string name)
        {
            try
            {
                action();
            }
            catch (Elr2ProtocolException)
            {
                return;
            }
            throw new Exception("assertion failed: " + name);
        }
    }
}
