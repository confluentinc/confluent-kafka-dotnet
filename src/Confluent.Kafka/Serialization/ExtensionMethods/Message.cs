namespace Confluent.Kafka.Serialization
{
    public static class MessageExtensions
    {
        public static Message<TKey, TValue> Deserialize<TKey, TValue>(this Message message, IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer)
            => new Message<TKey, TValue> (
                message.Topic,
                message.Partition,
                message.Offset,
                keyDeserializer.Deserialize(message.Key),
                valueDeserializer.Deserialize(message.Value),
                message.Timestamp,
                message.Error
            );
    }
}
