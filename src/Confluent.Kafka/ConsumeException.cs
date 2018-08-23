using System;

namespace Confluent.Kafka
{
    /// <summary>
    ///     Represents an error that occured during message consumption.
    /// </summary>
    public class ConsumeException : KafkaException
    {
        /// <summary>
        ///     Initialize a new instance of ConsumeException
        /// </summary>
        /// <param name="consumerRecord">
        ///     An object that provides information know about the consumer 
        ///     record for which the error occured.
        /// </param>
        /// <param name="error">
        ///     The error that occured.
        /// </param>
        public ConsumeException(ConsumeResult<byte[], byte[]> consumerRecord, Error error)
            : base(error)
        {
            ConsumerRecord = consumerRecord;
        }

        /// <summary>
        ///     An object that provides information known about the consumer
        ///     record for which the error occured.
        /// </summary>
        public ConsumeResult<byte[], byte[]> ConsumerRecord { get; private set; }
    }
}
