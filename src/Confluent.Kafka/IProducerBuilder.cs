using System;

namespace Confluent.Kafka
{
    public interface IProducerBuilder<TKey, TValue>
    {
        /// <summary>
        ///     Set the handler to call on statistics events. Statistics are provided as
        ///     a JSON formatted string as defined here:
        ///     https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md
        /// </summary>
        /// <remarks>
        ///     You can enable statistics and set the statistics interval
        ///     using the StatisticsIntervalMs configuration property
        ///     (disabled by default).
        ///
        ///     Executes on the poll thread (by default, a background thread managed by
        ///     the producer).
        ///
        ///     Exceptions: Any exception thrown by your statistics handler
        ///     will be devivered to your error handler, if set, else they will be
        ///     silently ignored.
        /// </remarks>
        ProducerBuilder<TKey, TValue> SetStatisticsHandler(Action<IProducer<TKey, TValue>, string> statisticsHandler);

        /// <summary>
        ///     Set a custom partitioner to use when producing messages to
        ///     <paramref name="topic" />.
        /// </summary>
        ProducerBuilder<TKey, TValue> SetPartitioner(string topic, PartitionerDelegate partitioner);

        /// <summary>
        ///     Set a custom partitioner that will be used for all topics
        ///     except those for which a partitioner has been explicitly configured.
        /// </summary>
        ProducerBuilder<TKey, TValue> SetDefaultPartitioner(PartitionerDelegate partitioner);

        /// <summary>
        ///     Set the handler to call on error events e.g. connection failures or all
        ///     brokers down. Note that the client will try to automatically recover from
        ///     errors that are not marked as fatal. Non-fatal errors should be interpreted
        ///     as informational rather than catastrophic.
        /// </summary>
        /// <remarks>
        ///     Executes on the poll thread (by default, a background thread managed by
        ///     the producer).
        ///
        ///     Exceptions: Any exception thrown by your error handler will be silently
        ///     ignored.
        /// </remarks>
        ProducerBuilder<TKey, TValue> SetErrorHandler(Action<IProducer<TKey, TValue>, Error> errorHandler);

        /// <summary>
        ///     Set the handler to call when there is information available
        ///     to be logged. If not specified, a default callback that writes
        ///     to stderr will be used.
        /// </summary>
        /// <remarks>
        ///     By default not many log messages are generated.
        ///
        ///     For more verbose logging, specify one or more debug contexts
        ///     using the Debug configuration property.
        ///
        ///     Warning: Log handlers are called spontaneously from internal
        ///     librdkafka threads and the application must not call any
        ///     Confluent.Kafka APIs from within a log handler or perform any
        ///     prolonged operations.
        ///
        ///     Exceptions: Any exception thrown by your log handler will be
        ///     silently ignored.
        /// </remarks>
        ProducerBuilder<TKey, TValue> SetLogHandler(Action<IProducer<TKey, TValue>, LogMessage> logHandler);

        /// <summary>
        ///     Set SASL/OAUTHBEARER token refresh callback in provided
        ///     conf object. The SASL/OAUTHBEARER token refresh callback
        ///     is triggered via <see cref="IProducer{TKey,TValue}.Poll"/>
        ///     whenever OAUTHBEARER is the SASL mechanism and a token
        ///     needs to be retrieved, typically based on the configuration
        ///     defined in sasl.oauthbearer.config. The callback should
        ///     invoke <see cref="ClientExtensions.OAuthBearerSetToken"/>
        ///     or <see cref="ClientExtensions.OAuthBearerSetTokenFailure"/>
        ///     to indicate success or failure, respectively.
        ///
        ///     An unsecured JWT refresh handler is provided by librdkafka
        ///     for development and testing purposes, it is enabled by
        ///     setting the enable.sasl.oauthbearer.unsecure.jwt property
        ///     to true and is mutually exclusive to using a refresh callback.
        /// </summary>
        /// <param name="oAuthBearerTokenRefreshHandler">
        ///     the callback to set; callback function arguments:
        ///     IConsumer - instance of the consumer which should be used to
        ///     set token or token failure string - Value of configuration
        ///     property sasl.oauthbearer.config
        /// </param>
        ProducerBuilder<TKey, TValue> SetOAuthBearerTokenRefreshHandler(Action<IProducer<TKey, TValue>, string> oAuthBearerTokenRefreshHandler);

        /// <summary>
        ///     The serializer to use to serialize keys.
        /// </summary>
        /// <remarks>
        ///     If your key serializer throws an exception, this will be
        ///     wrapped in a ProduceException with ErrorCode
        ///     Local_KeySerialization and thrown by the initiating call to
        ///     Produce or ProduceAsync.
        /// </remarks>
        ProducerBuilder<TKey, TValue> SetKeySerializer(ISerializer<TKey> serializer);

        /// <summary>
        ///     The serializer to use to serialize keys.
        /// </summary>
        /// <remarks>
        ///     If your key serializer throws an exception, this will be
        ///     wrapped in a ProduceException with ErrorCode
        ///     Local_KeySerialization and thrown by the initiating call to
        ///     Produce or ProduceAsync.
        /// </remarks>
        ProducerBuilder<TKey, TValue> SetKeySerializer(IAsyncSerializer<TKey> serializer);

        /// <summary>
        ///     The serializer to use to serialize values.
        /// </summary>
        /// <remarks>
        ///     If your value serializer throws an exception, this will be
        ///     wrapped in a ProduceException with ErrorCode
        ///     Local_ValueSerialization and thrown by the initiating call to
        ///     Produce or ProduceAsync.
        /// </remarks>
        ProducerBuilder<TKey, TValue> SetValueSerializer(ISerializer<TValue> serializer);

        /// <summary>
        ///     The serializer to use to serialize values.
        /// </summary>
        /// <remarks>
        ///     If your value serializer throws an exception, this will be
        ///     wrapped in a ProduceException with ErrorCode
        ///     Local_ValueSerialization and thrown by the initiating call to
        ///     Produce or ProduceAsync.
        /// </remarks>
        ProducerBuilder<TKey, TValue> SetValueSerializer(IAsyncSerializer<TValue> serializer);

        /// <summary>
        ///     Build a new IProducer implementation instance.
        /// </summary>
        IProducer<TKey, TValue> Build();
    }
}