namespace Confluent.Kafka.Serialization
{
    public class NullDeserializer : IDeserializer<Null>
    {
        public Null Deserialize(byte[] data)
        {
            return null;
        }
    }
}
