using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Elura.Protocol;

public static class GatewayRoutes
{
    public const uint Authenticate = 1;
    public const uint Heartbeat = 2;
    public const uint Reconnect = 3;
    public const uint SessionControl = 4;
    public const uint FirstApplication = 100;
}

public sealed record Identity(
    [property: JsonPropertyName("account_id")] long AccountId,
    [property: JsonPropertyName("user_id")] long UserId,
    [property: JsonPropertyName("region_id")] uint RegionId,
    [property: JsonPropertyName("realm_id")] uint RealmId,
    [property: JsonPropertyName("generation")] ulong Generation);

public sealed record AuthenticateRequest(
    [property: JsonPropertyName("ticket")] string Ticket);

public sealed record AuthenticateResponse(
    [property: JsonPropertyName("session_id")] string SessionId,
    [property: JsonPropertyName("identity")] Identity Identity,
    [property: JsonPropertyName("reconnect")] ReconnectTicketResponse Reconnect);

public sealed record ReconnectTicketRequest(
    [property: JsonPropertyName("ticket")] string Ticket);

public sealed record ReconnectTicketResponse(
    [property: JsonPropertyName("ticket")] string Ticket,
    [property: JsonPropertyName("expires_in_seconds")] ulong ExpiresInSeconds);

public sealed record ErrorEnvelope(
    [property: JsonPropertyName("code")] string Code,
    [property: JsonPropertyName("message")] string Message,
    [property: JsonPropertyName("retryable")] bool Retryable);

public static class GatewayPayloadCodec
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = false,
    };

    public static byte[] EncodeAuthenticateRequest(string ticket) =>
        JsonSerializer.SerializeToUtf8Bytes(new AuthenticateRequest(ticket), JsonOptions);

    public static AuthenticateResponse DecodeAuthenticateResponse(ReadOnlySpan<byte> payload) =>
        ValidateAuthenticationResponse(
            JsonSerializer.Deserialize<AuthenticateResponse>(payload, JsonOptions)
            ?? throw new Elr2ProtocolException("invalid authentication response"));

    // Renewal consumes the current reconnect ticket before returning its replacement.
    public static byte[] EncodeReconnectRequest(string ticket) =>
        JsonSerializer.SerializeToUtf8Bytes(new ReconnectTicketRequest(ticket), JsonOptions);

    public static ReconnectTicketResponse DecodeReconnectResponse(ReadOnlySpan<byte> payload) =>
        ValidateReconnectTicket(
            JsonSerializer.Deserialize<ReconnectTicketResponse>(payload, JsonOptions)
            ?? throw new Elr2ProtocolException("invalid reconnect response"));

    public static ErrorEnvelope DecodeError(ReadOnlySpan<byte> payload)
    {
        var envelope = JsonSerializer.Deserialize<ErrorEnvelope>(payload, JsonOptions)
            ?? throw new Elr2ProtocolException("invalid error envelope");
        if (envelope.Code.Length is 0 or > 64 ||
            Encoding.UTF8.GetByteCount(envelope.Message) > 1024 ||
            envelope.Code.Any(character =>
                character is not (>= 'A' and <= 'Z') and not (>= '0' and <= '9') and not '_'))
            throw new Elr2ProtocolException("invalid error envelope fields");
        return envelope;
    }

    private static AuthenticateResponse ValidateAuthenticationResponse(AuthenticateResponse response)
    {
        var identity = response.Identity;
        if (string.IsNullOrEmpty(response.SessionId) || identity.AccountId <= 0 || identity.UserId <= 0 ||
            identity.RegionId == 0 || identity.RealmId == 0 || identity.Generation == 0)
            throw new Elr2ProtocolException("invalid authentication response fields");
        ValidateReconnectTicket(response.Reconnect);
        return response;
    }

    private static ReconnectTicketResponse ValidateReconnectTicket(ReconnectTicketResponse response)
    {
        if (string.IsNullOrEmpty(response.Ticket) || response.ExpiresInSeconds == 0)
            throw new Elr2ProtocolException("invalid reconnect ticket fields");
        return response;
    }
}

public static class GatewayFrameRules
{
    public static void ValidateClientFrame(Elr2Frame frame, bool authenticated, ulong? pendingHeartbeat)
    {
        Elr2Codec.Validate(frame);
        if (frame.Kind == FrameKind.Response && frame.Route == GatewayRoutes.Heartbeat)
        {
            if (pendingHeartbeat is null || frame.RequestId != pendingHeartbeat ||
                frame.Sequence != 0 || frame.Payload.Length != 0)
                throw new Elr2ProtocolException("heartbeat response does not match an outstanding request");
            return;
        }
        if (frame.Kind != FrameKind.Request)
            throw new Elr2ProtocolException("Gateway accepts request frames and heartbeat responses only");
        if (frame.Route < GatewayRoutes.FirstApplication && frame.Sequence != 0)
            throw new Elr2ProtocolException("framework requests must have sequence zero");
        var allowed = authenticated
            ? frame.Route is GatewayRoutes.Heartbeat or GatewayRoutes.Reconnect ||
              frame.Route >= GatewayRoutes.FirstApplication
            : frame.Route == GatewayRoutes.Authenticate;
        if (!allowed)
            throw new Elr2ProtocolException("route is not allowed in the current session state");
    }

    public static Elr2Frame HeartbeatResponse(Elr2Frame request)
    {
        if (request.Kind != FrameKind.Request || request.Route != GatewayRoutes.Heartbeat ||
            request.Sequence != 0 || request.Payload.Length != 0)
            throw new Elr2ProtocolException("invalid heartbeat request");
        return new Elr2Frame(FrameKind.Response, 0, request.Route, request.RequestId, 0, []);
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

public sealed record SessionControl(SessionControlAction Action, string Reason);

public static class SessionControlCodec
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    public static byte[] Encode(SessionControl control)
    {
        if (!Enum.IsDefined(control.Action))
            throw new Elr2ProtocolException("unknown Session Control action");
        var reason = StrictUtf8.GetBytes(control.Reason);
        if (reason.Length > 256)
            throw new Elr2ProtocolException("Session Control reason exceeds 256 bytes");
        using var output = new MemoryStream();
        output.WriteByte(0x08);
        WriteVarint(output, (ulong)control.Action);
        if (reason.Length != 0)
        {
            output.WriteByte(0x12);
            WriteVarint(output, (ulong)reason.Length);
            output.Write(reason);
        }
        return output.ToArray();
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
                    throw new Elr2ProtocolException($"invalid Session Control UTF-8: {error.Message}");
                }
                offset += length;
            }
            else
            {
                SkipField(payload, ref offset, wireType);
            }
        }
        if (action is < 1 or > 5)
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
                _ = ReadVarint(input, ref offset);
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
                throw new Elr2ProtocolException("unsupported Session Control protobuf wire type");
        }
    }

    private static void SkipBytes(ReadOnlySpan<byte> input, ref int offset, int length)
    {
        if (length > input.Length - offset)
            throw new Elr2ProtocolException("truncated Session Control field");
        offset += length;
    }
}
