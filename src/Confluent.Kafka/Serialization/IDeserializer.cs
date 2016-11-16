
namespace Confluent.Kafka.Serialization
{
    public interface IDeserializer<T>
    {
        T Deserialize(byte[] data);
    }
}
