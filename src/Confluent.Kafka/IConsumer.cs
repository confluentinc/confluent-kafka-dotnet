using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka.Serialization;

namespace Confluent.Kafka
{
    /// <summary>
    ///     Implements a high-level Apache Kafka consumer (with 
    ///     key and value deserialization).
    /// </summary>
    public interface IConsumer<TKey, TValue> : IDisposable
    {
        /// <summary>
        ///     The IDeserializer implementation instance used to deserialize keys.
        /// </summary>
        IDeserializer<TKey> KeyDeserializer { get; }

        /// <summary>
        ///     The IDeserializer implementation instance used to deserialize values.
        /// </summary>
        IDeserializer<TValue> ValueDeserializer { get; }

        /// <summary>
        ///     Gets the current partition assignment as set by Assign.
        /// </summary>
        List<TopicPartition> Assignment { get; }

        /// <summary>
        ///     Gets the current partition subscription as set by Subscribe.
        /// </summary>
        List<string> Subscription { get; }

        /// <summary>
        ///     Gets the name of this consumer instance.
        ///     Contains (but is not equal to) the client.id configuration parameter.
        /// </summary>
        /// <remarks>
        ///     This name will be unique across all consumer instances
        ///     in a given application which allows log messages to be
        ///     associated with the corresponding instance.
        /// </remarks>
        string Name { get; }

        /// <summary>
        ///     Gets the (dynamic) group member id of this consumer (as set by
        ///     the broker).
        /// </summary>
        string MemberId { get; }

        /// <summary>
        ///     Poll for new messages / consumer events. Blocks until a new 
        ///     message or event is ready to be handled or the timeout period
        ///     <paramref name="millisecondsTimeout" /> has elapsed.
        ///     
        ///     [UNSTABLE-API] - prefer to use <see cref="Consumer{TKey,TValue}.Poll()"/> / 
        ///     <see cref="Consumer{TKey,TValue}.OnMessage"/> instead of Consume. We may remove
        ///     this method in the future to limit API surface area. 
        ///     Please let us know if you have a use case where Consume 
        ///     more convenient than Poll.
        /// </summary>
        /// <param name="message">
        ///     A consumed message, or null if no messages are 
        ///     available for consumption.
        /// </param>
        /// <param name="millisecondsTimeout">
        ///     The maximum time to block (in milliseconds), or -1 to 
        ///     block indefinitely. You should typically use a
        ///     relatively short timout period because this operation
        ///     cannot be cancelled.
        /// </param>
        /// <returns>
        ///     true: a message (with non-error state) was consumed.
        ///     false: no message was available for consumption.
        /// </returns>
        /// <remarks>
        ///     Will invoke events for OnPartitionsAssigned/Revoked,
        ///     OnOffsetsCommitted, OnConsumeError etc. on the calling 
        ///     thread.
        /// </remarks>
        bool Consume(out Message<TKey, TValue> message, int millisecondsTimeout);

        /// <summary>
        ///     Refer to <see cref="Consumer{TKey,TValue}.Consume(out Confluent.Kafka.Message{TKey,TValue},int)" />.
        ///     
        ///     [UNSTABLE-API] - prefer to use <see cref="Consumer{TKey,TValue}.Poll()"/> / <see cref="Consumer{TKey,TValue}.OnMessage"/> instead of this method.
        /// </summary>
        bool Consume(out Message<TKey, TValue> message, TimeSpan timeout);

        /// <summary>
        ///     Poll for new consumer events, including new messages
        ///     ready to be consumed (which will trigger the OnMessage
        ///     event). Blocks until a new event is available to be 
        ///     handled or the timeout period <paramref name="millisecondsTimeout" /> 
        ///     has elapsed.
        /// </summary>
        /// <param name="millisecondsTimeout"> 
        ///     The maximum time to block (in milliseconds), or -1 to 
        ///     block indefinitely. You should typically use a
        ///     relatively short timout period because this operation
        ///     cannot be cancelled.
        /// </param>
        void Poll(int millisecondsTimeout);

        /// <summary>
        ///     Poll for new consumer events, including new messages
        ///     ready to be consumed (which will trigger the OnMessage
        ///     event). Blocks until a new event is available to be
        ///     handled or the timeout period <paramref name="timeout" /> 
        ///     has elapsed.
        /// </summary>
        /// <param name="timeout"> 
        ///     The maximum time to block. You should typically use a
        ///     relatively short timout period because this operation
        ///     cannot be cancelled.
        /// </param>
        void Poll(TimeSpan timeout);

        /// <summary>
        ///     Raised on new partition assignment.
        ///     You should typically call the Consumer.Assign method in this handler.
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        event EventHandler<List<TopicPartition>> OnPartitionsAssigned;

        /// <summary>
        ///     Raised when a partition assignment is revoked.
        ///     You should typically call the Consumer.Unassign method in this handler.
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        event EventHandler<List<TopicPartition>> OnPartitionsRevoked;

        /// <summary>
        ///     Raised to report the result of (automatic) offset commits.
        ///     Not raised as a result of the use of the CommitAsync method.
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        event EventHandler<CommittedOffsets> OnOffsetsCommitted;

        /// <summary>
        ///     Raised when there is information that should be logged.
        /// </summary>
        /// <remarks>
        ///     Note: By default not many log messages are generated.
        /// 
        ///     You can specify one or more debug contexts using the 'debug'
        ///     configuration property and a log level using the 'log_level'
        ///     configuration property to enable more verbose logging,
        ///     however you shouldn't typically need to do this.
        ///
        ///     Warning: Log handlers are called spontaneously from internal librdkafka 
        ///     threads and the application must not call any Confluent.Kafka APIs from 
        ///     within a log handler or perform any prolonged operations.
        /// </remarks>
        event EventHandler<LogMessage> OnLog;

        /// <summary>
        ///     Raised on librdkafka statistics events. JSON formatted
        ///     string as defined here: https://github.com/edenhill/librdkafka/wiki/Statistics
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        event EventHandler<string> OnStatistics;

        /// <summary>
        ///     Raised when a consumed message has an error != NoError (both when Consume or Poll is used for polling).
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        event EventHandler<Message> OnConsumeError;

        /// <summary>
        ///     Raised on critical errors, e.g. connection failures or all 
        ///     brokers down. Note that the client will try to automatically 
        ///     recover from errors - these errors should be seen as 
        ///     informational rather than catastrophic
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        event EventHandler<Error> OnError;

        /// <summary>
        ///     Raised when the consumer reaches the end of a topic/partition it is reading from.
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        event EventHandler<TopicPartitionOffset> OnPartitionEOF;

        /// <summary>
        ///     Raised when a new message is avaiable for consumption. NOT raised when Consumer.Consume
        ///     is used for polling (only when Consmer.Poll is used for polling). NOT raised when the 
        ///     message has an Error (OnConsumeError is raised in that case).
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        event EventHandler<Message<TKey, TValue>> OnMessage;

        /// <summary>
        ///     Update the subscription set to topics.
        ///
        ///     Any previous subscription will be unassigned and unsubscribed first.
        ///
        ///     The subscription set denotes the desired topics to consume and this
        ///     set is provided to the partition assignor (one of the elected group
        ///     members) for all clients which then uses the configured
        ///     partition.assignment.strategy to assign the subscription sets's
        ///     topics's partitions to the consumers, depending on their subscription.
        /// </summary>
        void Subscribe(IEnumerable<string> topics);

        /// <summary>
        ///     Update the subscription set to a single topic.
        ///
        ///     Any previous subscription will be unassigned and unsubscribed first.
        /// </summary>
        void Subscribe(string topic);

        /// <summary>
        ///     Unsubscribe from the current subscription set.
        /// </summary>
        void Unsubscribe();

        /// <summary>
        ///     Update the assignment set to <paramref name="partitions" />.
        ///
        ///     The assignment set is the complete set of partitions to consume
        ///     from and will replace any previous assignment.
        /// </summary>
        /// <param name="partitions">
        ///     The set of partitions to consume from. If an offset value of
        ///     Offset.Invalid (-1001) is specified for a partition, consumption
        ///     will resume from the last committed offset on that partition, or
        ///     according to the 'auto.offset.reset' configuration parameter if
        ///     no offsets have been committed yet.
        /// </param>
        void Assign(IEnumerable<TopicPartitionOffset> partitions);

        /// <summary>
        ///     Update the assignment set to <paramref name="partitions" />.
        ///
        ///     The assignment set is the complete set of partitions to consume
        ///     from and will replace any previous assignment.
        /// </summary>
        /// <param name="partitions">
        ///     The set of partitions to consume from. Consumption will resume
        ///     from the last committed offset on each partition, or according
        ///     to the 'auto.offset.reset' configuration parameter if no offsets
        ///     have been committed yet.
        /// </param>
        void Assign(IEnumerable<TopicPartition> partitions);

        /// <summary>
        ///     Stop consumption and remove the current assignment.
        /// </summary>
        void Unassign();

        /// <summary>
        ///     Commit offsets for the current assignment.
        /// </summary>
        Task<CommittedOffsets> CommitAsync();

        /// <summary>
        ///     Commits an offset based on the topic/partition/offset of a message.
        ///     The next message to be read will be that following <paramref name="message" />.
        /// </summary>
        /// <param name="message">
        ///     The message used to determine the committed offset.
        /// </param>
        /// <remarks>
        ///     A consumer which has position N has consumed records with offsets 0 through N-1 and will next receive the record with offset N.
        ///     Hence, this method commits an offset of <paramref name="message" />.Offset + 1.
        /// </remarks>
        Task<CommittedOffsets> CommitAsync(Message<TKey, TValue> message);

        /// <summary>
        ///     Commit an explicit list of offsets.
        /// </summary>
        Task<CommittedOffsets> CommitAsync(IEnumerable<TopicPartitionOffset> offsets);

        /// <summary>
        ///     Retrieve current committed offsets for topics + partitions.
        ///
        ///     The offset field of each requested partition will be set to the offset
        ///     of the last consumed message, or RD_KAFKA_OFFSET_INVALID in case there was
        ///     no previous message, or, alternately a partition specific error may also be
        ///     returned.
        ///
        ///     throws KafkaException if there was a problem retrieving the above information.
        /// </summary>
        List<TopicPartitionOffsetError> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout);

        /// <summary>
        ///     Retrieve current positions (offsets) for topics + partitions.
        ///
        ///     The offset field of each requested partition will be set to the offset
        ///     of the last consumed message + 1, or RD_KAFKA_OFFSET_INVALID in case there was
        ///     no previous message, or, alternately a partition specific error may also be
        ///     returned.
        ///
        ///     throws KafkaException if there was a problem retrieving the above information.
        /// </summary>
        List<TopicPartitionOffsetError> Position(IEnumerable<TopicPartition> partitions);

        /// <summary>
        ///     Get information pertaining to all groups in the Kafka cluster (blocking).
        ///
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
        /// </param>
        List<GroupInfo> ListGroups(TimeSpan timeout);

        /// <summary>
        ///     Get information pertaining to a particular group in the
        ///     Kafka cluster (blocking).
        ///
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        /// <param name="group">
        ///     The group of interest.
        /// </param>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
        /// </param>
        /// <returns>
        ///     Returns information pertaining to the specified group
        ///     or null if this group does not exist.
        /// </returns>
        GroupInfo ListGroup(string group, TimeSpan timeout);

        /// <summary>
        ///     Get information pertaining to a particular group in the
        ///     Kafka cluster (blocks, potentially indefinitely).
        ///
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        /// <param name="group">
        ///     The group of interest.
        /// </param>
        /// <returns>
        ///     Returns information pertaining to the specified group
        ///     or null if this group does not exist.
        /// </returns>
        GroupInfo ListGroup(string group);

        /// <summary>
        ///     Get last known low (oldest/beginning) and high (newest/end)
        ///     offsets for a topic/partition.
        /// 
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        /// <remarks>
        ///     The low offset is updated periodically (if statistics.interval.ms is set)
        ///     while the high offset is updated on each fetched message set from the 
        ///     broker.
        ///
        ///     If there is no cached offset (either low or high, or both) then
        ///     Offset.Invalid will be returned for the respective offset.
        /// </remarks>
        /// <param name="topicPartition">
        ///     The topic/partition of interest.
        /// </param>
        /// <returns>
        ///     The requested WatermarkOffsets.
        /// </returns>
        WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition);

        /// <summary>
        ///     Query the Kafka cluster for low (oldest/beginning) and high (newest/end)
        ///     offsets for the specified topic/partition (blocking).
        ///
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        /// <param name="topicPartition">
        ///     The topic/partition of interest.
        /// </param>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
        /// </param>
        /// <returns>
        ///     The requested WatermarkOffsets.
        /// </returns>
        WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout);

        /// <summary>
        ///     Query the Kafka cluster for low (oldest/beginning) and high (newest/end)
        ///     offsets for the specified topic/partition (blocks, potentially indefinitely).
        ///
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        /// <param name="topicPartition">
        ///     The topic/partition of interest.
        /// </param>
        /// <returns>
        ///     The requested WatermarkOffsets.
        /// </returns>
        WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition);

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Producer.GetMetadata(bool,string,int)" /> for more information.
        ///     
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        Metadata GetMetadata(bool allTopics, TimeSpan timeout);

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Producer.GetMetadata(bool,string,int)" /> for more information.
        ///     
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        Metadata GetMetadata(bool allTopics);

        /// <summary>
        ///     Adds one or more brokers to the Consumer's list of initial
        ///     bootstrap brokers. 
        ///
        ///     Note: Additional brokers are discovered automatically as 
        ///     soon as the Consumer connects to any broker by querying the 
        ///     broker metadata. Calling this method is only required in 
        ///     some scenarios where the address of all brokers in the 
        ///     cluster changes.
        /// </summary>
        /// <param name="brokers">
        ///     Coma-separated list of brokers in the same format as 
        ///     the bootstrap.server configuration parameter.
        /// </param>
        /// <remarks>
        ///     There is currently no API to remove existing configured, 
        ///     added or learnt brokers.
        /// </remarks>
        /// <returns>
        ///     The number of brokers added. This value includes brokers
        ///     that may have been specified a second time.
        /// </returns>
        int AddBrokers(string brokers);
    }
}