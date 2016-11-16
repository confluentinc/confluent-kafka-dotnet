using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace Confluent.Kafka.Serialization
{
    public class NullDeserializer : IDeserializer<Null>
    {
        public Null Deserialize(byte[] data)
        {
            return Null.Instance;
        }
    }
}
