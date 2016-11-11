using System;

namespace Confluent.Kafka
{
    /// <summary>
    /// Used by the topics of the producer client to notify on produce request progress.
    /// </summary>
    /// <remarks>Methods of this interface will be executed in an RdKafka-internal thread and will block other operations - consider this when implementing.</remarks>
    public interface IDeliveryHandler
    {
        /// <summary>
        /// Invoked if an exception happens for the given produce request.
        /// </summary>
        /// <param name="exception"></param>
        void SetException(Exception exception);

        /// <summary>
        /// Invoked when the produce request successfully completes.
        /// </summary>
        /// <param name="deliveryReport"></param>
        void SetResult(DeliveryReport deliveryReport);
    }
}
