using System;

namespace Confluent.Kafka
{
    /// <remarks>
    ///     Methods of this interface will be executed on the poll thread and will
    ///     block other operations - consider this when implementing.
    /// </remarks>
    public interface IDeliveryHandler
    {
        bool MarshalData { get; }

        void HandleDeliveryReport(Message message);
    }

    /// <remarks>
    ///     Methods of this interface will be executed on the poll thread and will
    ///     block other operations - consider this when implementing.
    /// </remarks>
    public interface IDeliveryHandler<TKey, TValue>
    {
        bool MarshalData { get; }

        void HandleDeliveryReport(Message<TKey, TValue> message);
    }

}
