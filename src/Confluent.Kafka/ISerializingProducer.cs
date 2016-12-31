using System;
using System.Threading.Tasks;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka
{
    public interface ISerializingProducer<TKey, TValue>
    {
        string Name { get; }

        ISerializer<TKey> KeySerializer { get; }

        ISerializer<TValue> ValueSerializer { get; }

        Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, DateTime? timestamp = null, int? partition = null, bool blockIfQueueFull = true);

        void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler, DateTime? timestamp = null, int? partition = null, bool blockIfQueueFull = true);

        event EventHandler<LogMessage> OnLog;

        event EventHandler<Error> OnError;

        event EventHandler<string> OnStatistics;

    }
}
