namespace Confluent.Kafka.Serialization
{
    public class NullSerializer : ISerializer<Null>
    {
        public byte[] Serialize(Null val)
        {
            return null;
        }
    }
}
