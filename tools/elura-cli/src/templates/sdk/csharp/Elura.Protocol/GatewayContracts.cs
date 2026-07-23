using System;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Xml;

namespace Elura.Protocol
{
    public static class GatewayRoutes
    {
        public const uint Authenticate = 1;
        public const uint Heartbeat = 2;
        public const uint Reconnect = 3;
        public const uint SessionControl = 4;
        public const uint FirstApplication = 100;
    }

    [DataContract]
    public sealed class Identity
    {
        public Identity()
        {
        }

        public Identity(long accountId, long userId, uint regionId, uint realmId, ulong generation)
        {
            AccountId = accountId;
            UserId = userId;
            RegionId = regionId;
            RealmId = realmId;
            Generation = generation;
        }

        [DataMember(Name = "account_id", Order = 1)]
        public long AccountId { get; set; }

        [DataMember(Name = "user_id", Order = 2)]
        public long UserId { get; set; }

        [DataMember(Name = "region_id", Order = 3)]
        public uint RegionId { get; set; }

        [DataMember(Name = "realm_id", Order = 4)]
        public uint RealmId { get; set; }

        [DataMember(Name = "generation", Order = 5)]
        public ulong Generation { get; set; }
    }

    [DataContract]
    public sealed class AuthenticateRequest
    {
        public AuthenticateRequest()
        {
            Ticket = string.Empty;
        }

        public AuthenticateRequest(string ticket)
        {
            Ticket = ticket ?? throw new ArgumentNullException(nameof(ticket));
        }

        [DataMember(Name = "ticket", Order = 1)]
        public string Ticket { get; set; }
    }

    [DataContract]
    public sealed class AuthenticateResponse
    {
        public AuthenticateResponse()
        {
            SessionId = string.Empty;
            Identity = new Identity();
            Reconnect = new ReconnectTicketResponse();
        }

        public AuthenticateResponse(
            string sessionId,
            Identity identity,
            ReconnectTicketResponse reconnect)
        {
            SessionId = sessionId ?? throw new ArgumentNullException(nameof(sessionId));
            Identity = identity ?? throw new ArgumentNullException(nameof(identity));
            Reconnect = reconnect ?? throw new ArgumentNullException(nameof(reconnect));
        }

        [DataMember(Name = "session_id", Order = 1)]
        public string SessionId { get; set; }

        [DataMember(Name = "identity", Order = 2)]
        public Identity Identity { get; set; }

        [DataMember(Name = "reconnect", Order = 3)]
        public ReconnectTicketResponse Reconnect { get; set; }
    }

    [DataContract]
    public sealed class ReconnectTicketRequest
    {
        public ReconnectTicketRequest()
        {
            Ticket = string.Empty;
        }

        public ReconnectTicketRequest(string ticket)
        {
            Ticket = ticket ?? throw new ArgumentNullException(nameof(ticket));
        }

        [DataMember(Name = "ticket", Order = 1)]
        public string Ticket { get; set; }
    }

    [DataContract]
    public sealed class ReconnectTicketResponse
    {
        public ReconnectTicketResponse()
        {
            Ticket = string.Empty;
        }

        public ReconnectTicketResponse(string ticket, ulong expiresInSeconds)
        {
            Ticket = ticket ?? throw new ArgumentNullException(nameof(ticket));
            ExpiresInSeconds = expiresInSeconds;
        }

        [DataMember(Name = "ticket", Order = 1)]
        public string Ticket { get; set; }

        [DataMember(Name = "expires_in_seconds", Order = 2)]
        public ulong ExpiresInSeconds { get; set; }
    }

    [DataContract]
    public sealed class ErrorEnvelope
    {
        public ErrorEnvelope()
        {
            Code = string.Empty;
            Message = string.Empty;
        }

        public ErrorEnvelope(
            string code,
            string message,
            bool retryable,
            ulong? retryAfterMs = null)
        {
            Code = code ?? throw new ArgumentNullException(nameof(code));
            Message = message ?? throw new ArgumentNullException(nameof(message));
            Retryable = retryable;
            RetryAfterMs = retryAfterMs;
        }

        [DataMember(Name = "code", Order = 1)]
        public string Code { get; set; }

        [DataMember(Name = "message", Order = 2)]
        public string Message { get; set; }

        [DataMember(Name = "retryable", Order = 3)]
        public bool Retryable { get; set; }

        [DataMember(Name = "retry_after_ms", Order = 4, EmitDefaultValue = false)]
        public ulong? RetryAfterMs { get; set; }
    }

    public static class GatewayFrames
    {
        public static Elr2Frame Authenticate(ulong requestId, string ticket)
        {
            return Elr2Frame.Request(
                GatewayRoutes.Authenticate,
                requestId,
                GatewayPayloadCodec.EncodeAuthenticateRequest(ticket));
        }

        public static Elr2Frame Reconnect(ulong requestId, string ticket)
        {
            return Elr2Frame.Request(
                GatewayRoutes.Reconnect,
                requestId,
                GatewayPayloadCodec.EncodeReconnectRequest(ticket));
        }

        public static Elr2Frame HeartbeatResponse(Elr2Frame request)
        {
            return GatewayFrameRules.HeartbeatResponse(request);
        }
    }

    public static class GatewayPayloadCodec
    {
        public static byte[] EncodeAuthenticateRequest(string ticket)
        {
            return Serialize(new AuthenticateRequest(ticket));
        }

        public static AuthenticateResponse DecodeAuthenticateResponse(ReadOnlySpan<byte> payload)
        {
            return ValidateAuthenticationResponse(Deserialize<AuthenticateResponse>(payload));
        }

        // Renewal consumes the current reconnect ticket before returning its replacement.
        public static byte[] EncodeReconnectRequest(string ticket)
        {
            return Serialize(new ReconnectTicketRequest(ticket));
        }

        public static ReconnectTicketResponse DecodeReconnectResponse(ReadOnlySpan<byte> payload)
        {
            return ValidateReconnectTicket(Deserialize<ReconnectTicketResponse>(payload));
        }

        public static ErrorEnvelope DecodeError(ReadOnlySpan<byte> payload)
        {
            var envelope = Deserialize<ErrorEnvelope>(payload);
            if (string.IsNullOrEmpty(envelope.Code) ||
                envelope.Code.Length > 64 ||
                envelope.Message == null ||
                Encoding.UTF8.GetByteCount(envelope.Message) > 1024 ||
                envelope.RetryAfterMs == 0 ||
                !IsErrorCode(envelope.Code))
            {
                throw new Elr2ProtocolException("invalid error envelope fields");
            }
            return envelope;
        }

        private static byte[] Serialize<T>(T value)
        {
            try
            {
                using (var output = new MemoryStream())
                {
                    var serializer = new DataContractJsonSerializer(typeof(T));
                    serializer.WriteObject(output, value);
                    return output.ToArray();
                }
            }
            catch (SerializationException error)
            {
                throw new Elr2ProtocolException("invalid Gateway JSON payload", error);
            }
        }

        private static T Deserialize<T>(ReadOnlySpan<byte> payload)
            where T : class
        {
            try
            {
                using (var input = new MemoryStream(payload.ToArray(), false))
                {
                    var serializer = new DataContractJsonSerializer(typeof(T));
                    var result = serializer.ReadObject(input) as T;
                    if (result == null)
                        throw new Elr2ProtocolException("invalid Gateway JSON payload");
                    return result;
                }
            }
            catch (SerializationException error)
            {
                throw new Elr2ProtocolException("invalid Gateway JSON payload", error);
            }
            catch (XmlException error)
            {
                throw new Elr2ProtocolException("invalid Gateway JSON payload", error);
            }
        }

        private static bool IsErrorCode(string code)
        {
            foreach (var character in code)
            {
                if ((character < 'A' || character > 'Z') &&
                    (character < '0' || character > '9') &&
                    character != '_')
                {
                    return false;
                }
            }
            return true;
        }

        private static AuthenticateResponse ValidateAuthenticationResponse(
            AuthenticateResponse response)
        {
            var identity = response.Identity;
            if (string.IsNullOrEmpty(response.SessionId) ||
                identity == null ||
                identity.AccountId <= 0 ||
                identity.UserId <= 0 ||
                identity.RegionId == 0 ||
                identity.RealmId == 0 ||
                identity.Generation == 0 ||
                response.Reconnect == null)
            {
                throw new Elr2ProtocolException("invalid authentication response fields");
            }
            ValidateReconnectTicket(response.Reconnect);
            return response;
        }

        private static ReconnectTicketResponse ValidateReconnectTicket(
            ReconnectTicketResponse response)
        {
            if (string.IsNullOrEmpty(response.Ticket) || response.ExpiresInSeconds == 0)
                throw new Elr2ProtocolException("invalid reconnect ticket fields");
            return response;
        }
    }

    public static class GatewayFrameRules
    {
        public static void ValidateClientFrame(
            Elr2Frame frame,
            bool authenticated,
            ulong? pendingHeartbeat = null)
        {
            Elr2Codec.Validate(frame);
            if (frame.Kind == FrameKind.Response && frame.Route == GatewayRoutes.Heartbeat)
            {
                if (!pendingHeartbeat.HasValue ||
                    frame.RequestId != pendingHeartbeat.Value ||
                    frame.Sequence != 0 ||
                    frame.Payload.Length != 0)
                {
                    throw new Elr2ProtocolException(
                        "heartbeat response does not match an outstanding request");
                }
                return;
            }

            if (frame.Kind != FrameKind.Request)
            {
                throw new Elr2ProtocolException(
                    "Gateway accepts request frames and heartbeat responses only");
            }
            if (frame.Route < GatewayRoutes.FirstApplication && frame.Sequence != 0)
                throw new Elr2ProtocolException("framework requests must have sequence zero");

            var allowed = authenticated
                ? frame.Route == GatewayRoutes.Heartbeat ||
                  frame.Route == GatewayRoutes.Reconnect ||
                  frame.Route >= GatewayRoutes.FirstApplication
                : frame.Route == GatewayRoutes.Authenticate;
            if (!allowed)
                throw new Elr2ProtocolException("route is not allowed in the current session state");
        }

        public static Elr2Frame HeartbeatResponse(Elr2Frame request)
        {
            if (request == null)
                throw new ArgumentNullException(nameof(request));
            if (request.Kind != FrameKind.Request ||
                request.Route != GatewayRoutes.Heartbeat ||
                request.Sequence != 0 ||
                request.Payload.Length != 0)
            {
                throw new Elr2ProtocolException("invalid heartbeat request");
            }
            return Elr2Frame.Response(request);
        }
    }

    public enum SessionControlAction
    {
        Kick = 1,
        AccountVersionChanged = 2,
        DuplicateLogin = 3,
        ForceLogout = 4,
        ServerDraining = 5,
    }

    public sealed class SessionControl
    {
        public SessionControl(SessionControlAction action, string reason)
        {
            Action = action;
            Reason = reason ?? throw new ArgumentNullException(nameof(reason));
        }

        public SessionControlAction Action { get; }
        public string Reason { get; }
    }

    public static class SessionControlCodec
    {
        private static readonly UTF8Encoding StrictUtf8 = new UTF8Encoding(false, true);

        public static byte[] Encode(SessionControl control)
        {
            if (control == null)
                throw new ArgumentNullException(nameof(control));
            if (!Enum.IsDefined(typeof(SessionControlAction), control.Action))
                throw new Elr2ProtocolException("unknown Session Control action");
            var reason = StrictUtf8.GetBytes(control.Reason);
            if (reason.Length > 256)
                throw new Elr2ProtocolException("Session Control reason exceeds 256 bytes");

            using (var output = new MemoryStream())
            {
                output.WriteByte(0x08);
                WriteVarint(output, (ulong)control.Action);
                if (reason.Length != 0)
                {
                    output.WriteByte(0x12);
                    WriteVarint(output, (ulong)reason.Length);
                    output.Write(reason, 0, reason.Length);
                }
                return output.ToArray();
            }
        }

        public static SessionControl Decode(ReadOnlySpan<byte> payload)
        {
            var offset = 0;
            ulong action = 0;
            var reason = string.Empty;
            while (offset < payload.Length)
            {
                var tag = ReadVarint(payload, ref offset);
                var field = tag >> 3;
                var wireType = tag & 7;
                if (field == 0)
                    throw new Elr2ProtocolException("invalid Session Control protobuf tag");
                if (field == 1 && wireType == 0)
                {
                    action = ReadVarint(payload, ref offset);
                }
                else if (field == 2 && wireType == 2)
                {
                    var length = ReadLength(payload, ref offset);
                    try
                    {
                        reason = StrictUtf8.GetString(payload.Slice(offset, length));
                    }
                    catch (DecoderFallbackException error)
                    {
                        throw new Elr2ProtocolException(
                            "invalid Session Control UTF-8: " + error.Message,
                            error);
                    }
                    offset += length;
                }
                else
                {
                    SkipField(payload, ref offset, wireType);
                }
            }

            if (action < 1 || action > 5)
                throw new Elr2ProtocolException("unknown Session Control action");
            if (StrictUtf8.GetByteCount(reason) > 256)
                throw new Elr2ProtocolException("Session Control reason exceeds 256 bytes");
            return new SessionControl((SessionControlAction)action, reason);
        }

        private static void WriteVarint(Stream output, ulong value)
        {
            while (value >= 0x80)
            {
                output.WriteByte((byte)(value | 0x80));
                value >>= 7;
            }
            output.WriteByte((byte)value);
        }

        private static ulong ReadVarint(ReadOnlySpan<byte> input, ref int offset)
        {
            ulong value = 0;
            for (var shift = 0; shift < 64 && offset < input.Length; shift += 7)
            {
                var current = input[offset++];
                value |= (ulong)(current & 0x7f) << shift;
                if ((current & 0x80) == 0)
                    return value;
            }
            throw new Elr2ProtocolException("invalid Session Control protobuf varint");
        }

        private static int ReadLength(ReadOnlySpan<byte> input, ref int offset)
        {
            var length = ReadVarint(input, ref offset);
            if (length > int.MaxValue || (int)length > input.Length - offset)
                throw new Elr2ProtocolException("truncated Session Control field");
            return (int)length;
        }

        private static void SkipField(ReadOnlySpan<byte> input, ref int offset, ulong wireType)
        {
            switch (wireType)
            {
                case 0:
                    ReadVarint(input, ref offset);
                    return;
                case 1:
                    SkipBytes(input, ref offset, 8);
                    return;
                case 2:
                    SkipBytes(input, ref offset, ReadLength(input, ref offset));
                    return;
                case 5:
                    SkipBytes(input, ref offset, 4);
                    return;
                default:
                    throw new Elr2ProtocolException(
                        "unsupported Session Control protobuf wire type");
            }
        }

        private static void SkipBytes(ReadOnlySpan<byte> input, ref int offset, int length)
        {
            if (length > input.Length - offset)
                throw new Elr2ProtocolException("truncated Session Control field");
            offset += length;
        }
    }
}
