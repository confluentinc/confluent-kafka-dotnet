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

        void HandleDeliveryReport(MessageInfo messageInfo);
    }

    /// <remarks>
    ///     Methods of this interface will be executed on the poll thread and will
    ///     block other operations - consider this when implementing.
    /// </remarks>
    public interface IDeliveryHandler<TKey, TValue>
    {
        bool MarshalData { get; }

        void HandleDeliveryReport(MessageInfo<TKey, TValue> messageInfo);
    }

}
