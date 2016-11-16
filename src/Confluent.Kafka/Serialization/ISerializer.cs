namespace Confluent.Kafka.Serialization
{
    public interface ISerializer<T>
    {
        byte[] Serialize(T data);
    }
}
