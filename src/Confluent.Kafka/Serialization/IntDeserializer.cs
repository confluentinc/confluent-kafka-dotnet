namespace Confluent.Kafka.Serialization
{
    public class IntDeserializer : IDeserializer<int>
    {
        public int Deserialize(byte[] data)
        {
            // network byte order -> big endian -> most significant byte in the smallest address.
            return
                (((int)data[0]) << 24) |
                (((int)data[1]) << 16) |
                (((int)data[2]) << 8) |
                (int)data[3];
        }
    }
}
