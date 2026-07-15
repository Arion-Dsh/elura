using System.Buffers.Binary;

namespace Elura.Protocol;

public static class Elr2
{
    public const uint Magic = 0x454c5232;
    public const ushort Version = {{ELR2_VERSION}};
    public const int HeaderLength = 28;
    public const int DefaultMaxPayload = 1 << 20;
    public const int AbsoluteMaxPayload = 64 << 20;
    public const string ProtocolIdentifier = "{{PROTOCOL_IDENTIFIER}}";
}

public enum FrameKind : byte
{
    Request = 1,
    Response = 2,
    Push = 3,
    Error = 4,
}

public sealed record Elr2Frame(
    FrameKind Kind,
    byte Flags,
    uint Route,
    ulong RequestId,
    uint Sequence,
    byte[] Payload);

public sealed class Elr2ProtocolException(string message) : Exception(message);

public static class Elr2Codec
{
    public static void Validate(Elr2Frame frame, int maxPayload = Elr2.DefaultMaxPayload)
    {
        ValidateLimit(maxPayload);
        ArgumentNullException.ThrowIfNull(frame);
        ArgumentNullException.ThrowIfNull(frame.Payload);
        if (frame.Payload.Length > maxPayload)
            throw new Elr2ProtocolException("payload is too large");
        if (frame.Flags != 0)
            throw new Elr2ProtocolException("unsupported frame flags");
        if (frame.Route == 0)
            throw new Elr2ProtocolException("route must be non-zero");
        if (!Enum.IsDefined(frame.Kind))
            throw new Elr2ProtocolException("unknown frame kind");
        if (frame.Kind == FrameKind.Push)
        {
            if (frame.RequestId != 0)
                throw new Elr2ProtocolException("push request id must be zero");
        }
        else if (frame.RequestId == 0)
        {
            throw new Elr2ProtocolException("request id must be non-zero");
        }
    }

    public static byte[] Encode(Elr2Frame frame, int maxPayload = Elr2.DefaultMaxPayload)
    {
        Validate(frame, maxPayload);
        var output = new byte[checked(Elr2.HeaderLength + frame.Payload.Length)];
        var header = output.AsSpan(0, Elr2.HeaderLength);
        BinaryPrimitives.WriteUInt32BigEndian(header, Elr2.Magic);
        BinaryPrimitives.WriteUInt16BigEndian(header[4..], Elr2.Version);
        header[6] = (byte)frame.Kind;
        header[7] = frame.Flags;
        BinaryPrimitives.WriteUInt32BigEndian(header[8..], frame.Route);
        BinaryPrimitives.WriteUInt64BigEndian(header[12..], frame.RequestId);
        BinaryPrimitives.WriteUInt32BigEndian(header[20..], frame.Sequence);
        BinaryPrimitives.WriteUInt32BigEndian(header[24..], (uint)frame.Payload.Length);
        frame.Payload.CopyTo(output, Elr2.HeaderLength);
        return output;
    }

    public static Elr2Frame Decode(ReadOnlySpan<byte> message, int maxPayload = Elr2.DefaultMaxPayload)
    {
        ValidateLimit(maxPayload);
        if (message.Length < Elr2.HeaderLength)
            throw new Elr2ProtocolException("incomplete Elura frame");
        if (BinaryPrimitives.ReadUInt32BigEndian(message) != Elr2.Magic)
            throw new Elr2ProtocolException("invalid Elura magic");
        if (BinaryPrimitives.ReadUInt16BigEndian(message[4..]) != Elr2.Version)
            throw new Elr2ProtocolException("unsupported Elura version");
        var kind = (FrameKind)message[6];
        if (!Enum.IsDefined(kind))
            throw new Elr2ProtocolException("unknown frame kind");
        var payloadLength = BinaryPrimitives.ReadUInt32BigEndian(message[24..]);
        if (payloadLength > maxPayload)
            throw new Elr2ProtocolException("Elura payload is too large");
        if (message.Length != checked(Elr2.HeaderLength + (int)payloadLength))
            throw new Elr2ProtocolException("Elura message must contain exactly one frame");
        var frame = new Elr2Frame(
            kind,
            message[7],
            BinaryPrimitives.ReadUInt32BigEndian(message[8..]),
            BinaryPrimitives.ReadUInt64BigEndian(message[12..]),
            BinaryPrimitives.ReadUInt32BigEndian(message[20..]),
            message[Elr2.HeaderLength..].ToArray());
        Validate(frame, maxPayload);
        return frame;
    }

    internal static void ValidateLimit(int maxPayload)
    {
        if (maxPayload is <= 0 or > Elr2.AbsoluteMaxPayload)
            throw new ArgumentOutOfRangeException(nameof(maxPayload), "max payload must be in 1..=64MiB");
    }
}

public sealed class Elr2StreamDecoder
{
    private readonly int _maxPayload;
    private byte[] _buffer = new byte[4096];
    private int _count;

    public Elr2StreamDecoder(int maxPayload = Elr2.DefaultMaxPayload)
    {
        Elr2Codec.ValidateLimit(maxPayload);
        _maxPayload = maxPayload;
    }

    public int BufferedBytes => _count;

    public void Append(ReadOnlySpan<byte> chunk)
    {
        var required = checked(_count + chunk.Length);
        if (required > _buffer.Length)
            Array.Resize(ref _buffer, Math.Max(required, checked(_buffer.Length * 2)));
        chunk.CopyTo(_buffer.AsSpan(_count));
        _count = required;
    }

    public bool TryRead(out Elr2Frame? frame)
    {
        frame = null;
        if (_count < Elr2.HeaderLength)
            return false;
        var header = _buffer.AsSpan(0, Elr2.HeaderLength);
        if (BinaryPrimitives.ReadUInt32BigEndian(header) != Elr2.Magic)
            throw new Elr2ProtocolException("invalid Elura magic");
        if (BinaryPrimitives.ReadUInt16BigEndian(header[4..]) != Elr2.Version)
            throw new Elr2ProtocolException("unsupported Elura version");
        var payloadLength = BinaryPrimitives.ReadUInt32BigEndian(header[24..]);
        if (payloadLength > _maxPayload)
            throw new Elr2ProtocolException("Elura payload is too large");
        var total = checked(Elr2.HeaderLength + (int)payloadLength);
        if (_count < total)
            return false;
        frame = Elr2Codec.Decode(_buffer.AsSpan(0, total), _maxPayload);
        Buffer.BlockCopy(_buffer, total, _buffer, 0, _count - total);
        _count -= total;
        return true;
    }
}
