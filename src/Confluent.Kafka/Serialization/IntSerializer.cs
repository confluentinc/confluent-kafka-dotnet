
// TODO: Consider whether we want to allow this serializer to handle nulls. Perhaps a separate serialzer that can?

namespace Confluent.Kafka.Serialization
{
    public class IntSerializer : ISerializer<int>
    {
        public byte[] Serialize(int val)
        {
            var result = new byte[4]; // int is always 32 bits on .NET.
            // network byte order -> big endian -> most significant byte in the smallest address.
            // Note: At the IL level, the conv.u1 operator is used to cast int to byte which truncates
            // the high order bits if overflow occurs.
            // https://msdn.microsoft.com/en-us/library/system.reflection.emit.opcodes.conv_u1.aspx
            result[0] = (byte)(val >> 24);
            result[1] = (byte)(val >> 16); // & 0xff;
            result[2] = (byte)(val >> 8); // & 0xff;
            result[3] = (byte)val; // & 0xff;
            return result;
        }
    }
}
