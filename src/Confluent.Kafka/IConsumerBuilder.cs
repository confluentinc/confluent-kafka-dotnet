using System;
using System.Collections.Generic;

namespace Confluent.Kafka
{
    public interface IConsumerBuilder<TKey, TValue>
    {
        /// <summary>
        ///     Set the handler to call on statistics events. Statistics 
        ///     are provided as a JSON formatted string as defined here:
        ///     https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md
        /// </summary>
        /// <remarks>
        ///     You can enable statistics and set the statistics interval
        ///     using the StatisticsIntervalMs configuration property
        ///     (disabled by default).
        ///
        ///     Executes as a side-effect of the Consume method (on the same
        ///     thread).
        ///
        ///     Exceptions: Any exception thrown by your statistics handler
        ///     will be wrapped in a ConsumeException with ErrorCode
        ///     ErrorCode.Local_Application and thrown by the initiating call
        ///     to Consume.
        /// </remarks>
        ConsumerBuilder<TKey, TValue> SetStatisticsHandler(
            Action<IConsumer<TKey, TValue>, string> statisticsHandler);

        /// <summary>
        ///     Set the handler to call on error events e.g. connection failures or all
        ///     brokers down. Note that the client will try to automatically recover from
        ///     errors that are not marked as fatal. Non-fatal errors should be interpreted
        ///     as informational rather than catastrophic.
        /// </summary>
        /// <remarks>
        ///     Executes as a side-effect of the Consume method (on the same thread).
        ///
        ///     Exceptions: Any exception thrown by your error handler will be silently
        ///     ignored.
        /// </remarks>
        ConsumerBuilder<TKey, TValue> SetErrorHandler(
            Action<IConsumer<TKey, TValue>, Error> errorHandler);

        /// <summary>
        ///     Set the handler to call when there is information available
        ///     to be logged. If not specified, a default callback that writes
        ///     to stderr will be used.
        /// </summary>
        /// <remarks>
        ///     By default not many log messages are generated.
        ///
        ///     For more verbose logging, specify one or more debug contexts
        ///     using the 'Debug' configuration property.
        ///
        ///     Warning: Log handlers are called spontaneously from internal
        ///     librdkafka threads and the application must not call any
        ///     Confluent.Kafka APIs from within a log handler or perform any
        ///     prolonged operations.
        ///
        ///     Exceptions: Any exception thrown by your log handler will be
        ///     silently ignored.
        /// </remarks>
        ConsumerBuilder<TKey, TValue> SetLogHandler(
            Action<IConsumer<TKey, TValue>, LogMessage> logHandler);

        /// <summary>
        ///     Set SASL/OAUTHBEARER token refresh callback in provided
        ///     conf object. The SASL/OAUTHBEARER token refresh callback
        ///     is triggered via <see cref="IConsumer{TKey,TValue}.Consume(int)"/>
        ///     (or any of its overloads) whenever OAUTHBEARER is the SASL
        ///     mechanism and a token needs to be retrieved, typically
        ///     based on the configuration defined in
        ///     sasl.oauthbearer.config. The callback should invoke
        ///     <see cref="ClientExtensions.OAuthBearerSetToken"/>
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
        ConsumerBuilder<TKey, TValue> SetOAuthBearerTokenRefreshHandler(Action<IConsumer<TKey, TValue>, string> oAuthBearerTokenRefreshHandler);

        /// <summary>
        ///     Set the deserializer to use to deserialize keys.
        /// </summary>
        /// <remarks>
        ///     If your key deserializer throws an exception, this will be
        ///     wrapped in a ConsumeException with ErrorCode
        ///     Local_KeyDeserialization and thrown by the initiating call to
        ///     Consume.
        /// </remarks>
        ConsumerBuilder<TKey, TValue> SetKeyDeserializer(IDeserializer<TKey> deserializer);

        /// <summary>
        ///     Set the deserializer to use to deserialize values.
        /// </summary>
        /// <remarks>
        ///     If your value deserializer throws an exception, this will be
        ///     wrapped in a ConsumeException with ErrorCode
        ///     Local_ValueDeserialization and thrown by the initiating call to
        ///     Consume.
        /// </remarks>
        ConsumerBuilder<TKey, TValue> SetValueDeserializer(IDeserializer<TValue> deserializer);

        /// <summary>
        ///     Specify a handler that will be called when a new consumer group partition assignment has
        ///     been received by this consumer.
        ///
        ///     The actual partitions to consume from and start offsets are specified by the return value
        ///     of the handler. Partition offsets may be a specific offset, or special value (Beginning, End
        ///     or Unset). If Unset, consumption will resume from the last committed offset for each
        ///     partition, or if there is no committed offset, in accordance with the `auto.offset.reset`
        ///     configuration property.
        ///
        ///     Kafka supports two rebalance protocols: EAGER (range and roundrobin assignors) and
        ///     COOPERATIVE (incremental) (cooperative-sticky assignor). Use the PartitionAssignmentStrategy
        ///     configuration property to specify which assignor to use.
        ///
        ///     ## EAGER Rebalancing (range, roundrobin)
        ///
        ///     The set of partitions returned from your handler may differ from that provided by the
        ///     group (though they should typically be the same). These partitions are the
        ///     entire set of partitions to consume from. There will be exactly one call to the
        ///     partitions revoked or partitions lost handler (if they have been set using
        ///     SetPartitionsRevokedHandler / SetPartitionsLostHandler) corresponding to every call to
        ///     this handler.
        ///
        ///     ## COOPERATIVE (Incremental) Rebalancing
        ///
        ///     The set of partitions returned from your handler must match that provided by the
        ///     group. These partitions are an incremental assignment - are in addition to those
        ///     already being consumed from.
        /// </summary>
        /// <remarks>
        ///     Executes as a side-effect of the Consumer.Consume call (on the same thread).
        ///     
        ///     (Incremental)Assign/Unassign must not be called in the handler.
        ///
        ///     Exceptions: Any exception thrown by your partitions assigned handler will be wrapped
        ///     in a ConsumeException with ErrorCode ErrorCode.Local_Application and thrown by the
        ///     initiating call to Consume.
        /// </remarks>
        ConsumerBuilder<TKey, TValue> SetPartitionsAssignedHandler(
            Func<IConsumer<TKey, TValue>, List<TopicPartition>, IEnumerable<TopicPartitionOffset>> partitionsAssignedHandler);

        /// <summary>
        ///     Specify a handler that will be called when a new consumer group partition assignment has
        ///     been received by this consumer.
        ///
        ///     Following execution of the handler, consumption will resume from the last committed offset
        ///     for each partition, or if there is no committed offset, in accordance with the
        ///     `auto.offset.reset` configuration property.
        ///
        ///     Kafka supports two rebalance protocols: EAGER (range and roundrobin assignors) and
        ///     COOPERATIVE (incremental) (cooperative-sticky assignor). Use the PartitionAssignmentStrategy
        ///     configuration property to specify which assignor to use.
        ///
        ///     ## EAGER Rebalancing (range, roundrobin)
        ///
        ///     Partitions passed to the handler represent the entire set of partitions to consume from.
        ///     There will be exactly one call to the partitions revoked or partitions lost handler (if
        ///     they have been set using SetPartitionsRevokedHandler / SetPartitionsLostHandler)
        ///     corresponding to every call to this handler.
        ///
        ///     ## COOPERATIVE (Incremental) Rebalancing
        ///
        ///     Partitions passed to the handler are an incremental assignment - are in addition to those
        ///     already being consumed from.
        /// </summary>
        /// <remarks>
        ///     Executes as a side-effect of the Consumer.Consume call (on the same thread).
        ///
        ///     (Incremental)Assign/Unassign must not be called in the handler.
        ///
        ///     Exceptions: Any exception thrown by your partitions assigned handler will be wrapped
        ///     in a ConsumeException with ErrorCode ErrorCode.Local_Application and thrown by the
        ///     initiating call to Consume.
        /// </remarks>
        ConsumerBuilder<TKey, TValue> SetPartitionsAssignedHandler(
            Action<IConsumer<TKey, TValue>, List<TopicPartition>> partitionAssignmentHandler);

        /// <summary>
        ///     Specify a handler that will be called immediately prior to the consumer's current assignment
        ///     being revoked, allowing the application to take action (e.g. offsets committed to a custom
        ///     store) before the consumer gives up ownership of the partitions. The Func partitions revoked
        ///     handler variant is not supported in the incremental rebalancing (COOPERATIVE) case.
        ///
        ///     The value returned from your handler specifies the partitions/offsets the consumer should
        ///     be assigned to read from following completion of this method (most typically empty). This
        ///     partitions revoked handler variant may not be specified when incremental rebalancing is in use
        ///     - in that case, the set of partitions the consumer is reading from may never deviate from
        ///     the set that it has been assigned by the group.
        ///
        ///     The second parameter provided to the handler provides the set of partitions the consumer is
        ///     currently assigned to, and the current position of the consumer on each of these partitions.
        /// </summary>
        /// <remarks>
        ///     Executes as a side-effect of the Consumer.Consume/Close/Dispose call (on the same thread).
        ///
        ///     (Incremental)Assign/Unassign must not be called in the handler.
        ///
        ///     Exceptions: Any exception thrown by your partitions revoked handler will be wrapped in a
        ///     ConsumeException with ErrorCode ErrorCode.Local_Application and thrown by the initiating call
        ///     to Consume/Close.
        /// </remarks>
        ConsumerBuilder<TKey, TValue> SetPartitionsRevokedHandler(
            Func<IConsumer<TKey, TValue>, List<TopicPartitionOffset>, IEnumerable<TopicPartitionOffset>> partitionsRevokedHandler);

        /// <summary>
        ///     Specify a handler that will be called immediately prior to partitions being revoked
        ///     from the consumer's current assignment, allowing the application to take action
        ///     (e.g. commit offsets to a custom store) before the consumer gives up ownership of
        ///     the partitions.
        ///
        ///     Kafka supports two rebalance protocols: EAGER (range and roundrobin assignors) and
        ///     COOPERATIVE (incremental) (cooperative-sticky assignor). Use the PartitionAssignmentStrategy
        ///     configuration property to specify which assignor to use.
        ///
        ///     ## EAGER Rebalancing (range, roundrobin)
        ///
        ///     The second parameter provides the entire set of partitions the consumer is currently
        ///     assigned to, and the current position of the consumer on each of these partitions.
        ///     The consumer will stop consuming from all partitions following execution of this
        ///     handler.
        ///
        ///     ## COOPERATIVE (Incremental) Rebalancing
        ///
        ///     The second parameter provides the subset of the partitions assigned to the consumer
        ///     which are being revoked, and the current position of the consumer on each of these
        ///     partitions. The consumer will stop consuming from this set of partitions following
        ///     execution of this handler, and continue reading from any remaining partitions.
        /// </summary>
        /// <remarks>
        ///     May execute as a side-effect of the Consumer.Consume/Close/Dispose call (on the same
        ///     thread).
        ///
        ///     (Incremental)Assign/Unassign must not be called in the handler.
        ///
        ///     Exceptions: Any exception thrown by your partitions revoked handler will be wrapped
        ///     in a ConsumeException with ErrorCode ErrorCode.Local_Application and thrown by the
        ///     initiating call to Consume.
        /// </remarks>
        ConsumerBuilder<TKey, TValue> SetPartitionsRevokedHandler(
            Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>> partitionsRevokedHandler);

        /// <summary>
        ///     Specify a handler that will be called when the consumer detects that it has lost ownership
        ///     of its partition assignment (fallen out of the group). The application should not commit
        ///     offsets in this case, since the partitions will likely be owned by other consumers in the
        ///     group (offset commits to Kafka will likely fail).
        ///
        ///     The value returned from your handler specifies the partitions/offsets the consumer should
        ///     be assigned to read from following completion of this method (most typically empty). This
        ///     partitions lost handler variant may not be specified when incremental rebalancing is in use
        ///     - in that case, the set of partitions the consumer is reading from may never deviate from
        ///     the set that it has been assigned by the group.
        ///
        ///     The second parameter provided to the handler provides the set of all partitions the consumer
        ///     is currently assigned to, and the current position of the consumer on each of these partitions.
        ///     Following completion of this handler, the consumer will stop consuming from all partitions.
        ///
        ///     If this handler is not specified, the partitions revoked handler (if specified) will be called
        ///     instead if partitions are lost.
        /// </summary>
        /// <remarks>
        ///     May execute as a side-effect of the Consumer.Consume/Close/Dispose call (on the same
        ///     thread).
        ///
        ///     (Incremental)Assign/Unassign must not be called in the handler.
        ///
        ///     Exceptions: Any exception thrown by your partitions revoked handler will be wrapped
        ///     in a ConsumeException with ErrorCode ErrorCode.Local_Application and thrown by the
        ///     initiating call to Consume.
        /// </remarks>
        ConsumerBuilder<TKey, TValue> SetPartitionsLostHandler(
            Func<IConsumer<TKey, TValue>, List<TopicPartitionOffset>, IEnumerable<TopicPartitionOffset>> partitionsLostHandler);

        /// <summary>
        ///     Specify a handler that will be called when the consumer detects that it has lost ownership
        ///     of its partition assignment (fallen out of the group). The application should not commit
        ///     offsets in this case, since the partitions will likely be owned by other consumers in the
        ///     group (offset commits to Kafka will likely fail).
        ///
        ///     The second parameter provided to the handler provides the set of all partitions the consumer
        ///     is currently assigned to, and the current position of the consumer on each of these partitions.
        ///
        ///     If this handler is not specified, the partitions revoked handler (if specified) will be called
        ///     instead if partitions are lost.
        /// </summary>
        /// <remarks>
        ///     May execute as a side-effect of the Consumer.Consume/Close/Dispose call (on the same
        ///     thread).
        ///
        ///     (Incremental)Assign/Unassign must not be called in the handler.
        ///
        ///     Exceptions: Any exception thrown by your partitions revoked handler will be wrapped
        ///     in a ConsumeException with ErrorCode ErrorCode.Local_Application and thrown by the
        ///     initiating call to Consume.
        /// </remarks>
        ConsumerBuilder<TKey, TValue> SetPartitionsLostHandler(
            Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>> partitionsLostHandler);

        /// <summary>
        ///     A handler that is called to report the result of (automatic) offset 
        ///     commits. It is not called as a result of the use of the Commit method.
        /// </summary>
        /// <remarks>
        ///     Executes as a side-effect of the Consumer.Consume call (on the same thread).
        ///
        ///     Exceptions: Any exception thrown by your offsets committed handler
        ///     will be wrapped in a ConsumeException with ErrorCode
        ///     ErrorCode.Local_Application and thrown by the initiating call to Consume/Close.
        /// </remarks>
        ConsumerBuilder<TKey, TValue> SetOffsetsCommittedHandler(Action<IConsumer<TKey, TValue>, CommittedOffsets> offsetsCommittedHandler);

        /// <summary>
        ///     Build a new IConsumer implementation instance.
        /// </summary>
        IConsumer<TKey, TValue> Build();
    }
}