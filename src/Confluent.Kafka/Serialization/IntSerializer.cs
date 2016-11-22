
namespace Confluent.Kafka.Serialization
{
    public class IntSerializer : ISerializer<int>
    {
        public byte[] Serialize(int val)
        {
            var result = new byte[4]; // int is always 32 bits on .NET.
            // network byte order -> big endian -> most significant byte in the smallest address.
            result[0] = (byte)(val >> 24);
            result[1] = (byte)(val >> 16);
            result[2] = (byte)(val >> 8);
            result[3] = (byte)val;
            return result;
        }
    }
}
