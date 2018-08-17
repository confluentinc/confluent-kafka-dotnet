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
        /// <param name="error">
        ///     The error that occured.
        /// </param>
        public ConsumeException(ConsumeResult<byte[], byte[]> consumerRecord, Error error)
        {
            ConsumerRecord = consumerRecord;
            Error = error;
        }

        /// <summary>
        ///     An object that provides information known about the consumer
        ///     record for which the error occured.
        /// </summary>
        public ConsumeResult<byte[], byte[]> ConsumerRecord { get; private set; }

        /// <summary>
        ///     The error that occurred.
        /// </summary>
        public Error Error { get; private set; }
    }
}
