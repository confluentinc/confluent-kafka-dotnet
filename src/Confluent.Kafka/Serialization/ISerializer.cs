namespace Confluent.Kafka.Serialization
{
    // TODO: Consider not having this. Consider replacing with Func<T, TResult>. This would be more normal, and I don't think we need the flexibility provided by having an interface.
    public interface ISerializer<T>
    {
        byte[] Serialize(T data);
    }
}
