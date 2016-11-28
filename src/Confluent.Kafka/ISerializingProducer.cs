using System.Threading.Tasks;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka
{
    public interface ISerializingProducer<TKey, TValue>
    {
        string Name { get; }

        ISerializer<TKey> KeySerializer { get; }

        ISerializer<TValue> ValueSerializer { get; }

        Task<DeliveryReport> ProduceAsync(string topic, TKey key, TValue val, int? partition = null, bool blockIfQueueFull = true);
    }
}
