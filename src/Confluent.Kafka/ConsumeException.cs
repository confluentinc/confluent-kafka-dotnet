using System;

namespace Confluent.Kafka
{
    /// <summary>
    ///     Represents an error that occured during message consumption.
    /// </summary>
    public class ConsumeException : Exception
    {
        /// <summary>
        ///     Initialize a new instance of ConsumeException
        /// </summary>
        /// <param name="consumerRecord">
        ///     An object that provides information know about the consumer 
        ///     record for which the error occured.
        /// </param>
        public ConsumeException(ConsumerRecord<byte[], byte[]> consumerRecord)
        {
            ConsumerRecord = consumerRecord;
        }

        /// <summary>
        ///     An object that provides information known about the consumer
        ///     record for which the error occured.
        /// </summary>
        public ConsumerRecord<byte[], byte[]> ConsumerRecord { get; }

        /// <summary>
        ///     A convenience property that provides a duplicate of the error
        ///     provided by ConsumerRecord.
        /// </summary>
        public Error Error { get => ConsumerRecord.Error; }
    }
}
