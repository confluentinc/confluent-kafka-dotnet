namespace Confluent.Kafka.Serialization
{
    public class EmptySerializer : ISerializer<Empty>
    {
        public static byte[] result = new byte[0];
        public byte[] Serialize(Empty val)
        {
            return result;
        }
    }
}
