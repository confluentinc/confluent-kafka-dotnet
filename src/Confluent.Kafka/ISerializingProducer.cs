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

        Task<MessageInfo<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, int? partition = null, bool blockIfQueueFull = true);
        Task<MessageInfo<TKey, TValue>> ProduceAsync(string topic, Message<TKey, TValue> message, int? partition = null, bool blockIfQueueFull = true);

        void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler, int? partition = null, bool blockIfQueueFull = true);
        void ProduceAsync(string topic, Message<TKey, TValue> message, IDeliveryHandler<TKey, TValue> deliveryHandler, int? partition = null, bool blockIfQueueFull = true);

        event EventHandler<LogArgs> OnLog;

        event EventHandler<ErrorArgs> OnError;

        event EventHandler<string> OnStatistics;

    }
}
