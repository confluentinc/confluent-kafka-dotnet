// Copyright 2018 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.Threading;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Defines a high-level Apache Kafka consumer
    ///     (with key and value deserialization).
    /// </summary>
    public interface IConsumer<TKey, TValue> : IClient
    {
        /// <summary>
        ///     Poll for new messages / events. Blocks
        ///     until a consume result is available or the
        ///     timeout period has elapsed.
        /// </summary>
        /// <param name="millisecondsTimeout">
        ///     The maximum period of time (in milliseconds)
        ///     the call may block.
        /// </param>
        /// <returns>
        ///     The consume result.
        /// </returns>
        /// <remarks>
        ///     The partitions assigned/revoked and offsets
        ///     committed handlers may be invoked as a
        ///     side-effect of calling this method (on the
        ///     same thread).
        /// </remarks>
        /// <exception cref="ConsumeException">
        ///     Thrown
        ///     when a call to this method is unsuccessful
        ///     for any reason. Inspect the Error property
        ///     of the exception for detailed information.
        /// </exception>
        ConsumeResult<TKey, TValue> Consume(int millisecondsTimeout);

        /// <summary>
        ///     Poll for new messages / events. Blocks
        ///     until a consume result is available or the
        ///     operation has been cancelled.
        /// </summary>
        /// <param name="cancellationToken">
        ///     A cancellation token
        ///     that can be used to cancel this operation.
        /// </param>
        /// <returns>
        ///     The consume result.
        /// </returns>
        /// <remarks>
        ///     The partitions assigned/revoked and
        ///     offsets committed handlers may be invoked
        ///     as a side-effect of calling this method
        ///     (on the same thread).
        /// </remarks>
        /// <exception cref="Confluent.Kafka.ConsumeException">
        ///     Thrown
        ///     when a call to this method is unsuccessful
        ///     for any reason (except cancellation by
        ///     user). Inspect the Error property of the
        ///     exception for detailed information.
        /// </exception>
        /// <exception cref="System.OperationCanceledException">
        ///     Thrown on cancellation.
        /// </exception>
        ConsumeResult<TKey, TValue> Consume(CancellationToken cancellationToken = default(CancellationToken));


        /// <summary>
        ///     Poll for new messages / events. Blocks
        ///     until a consume result is available or the
        ///     timeout period has elapsed.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum period of time
        ///     the call may block.
        /// </param>
        /// <returns>
        ///     The consume result.
        /// </returns>
        /// <remarks>
        ///     The partitions assigned/revoked and offsets
        ///     committed handlers may be invoked as a
        ///     side-effect of calling this method (on the
        ///     same thread).
        /// </remarks>
        /// <exception cref="ConsumeException">
        ///     Thrown
        ///     when a call to this method is unsuccessful
        ///     for any reason. Inspect the Error property
        ///     of the exception for detailed information.
        /// </exception>
        ConsumeResult<TKey, TValue> Consume(TimeSpan timeout);


        /// <summary>
        ///     Gets the (dynamic) group member id of
        ///     this consumer (as set by the broker).
        /// </summary>
        string MemberId { get; }


        /// <summary>
        ///     Gets the current partition assignment as set by
        ///     <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Assign(TopicPartition)" />
        ///     or implicitly.
        /// </summary>
        List<TopicPartition> Assignment { get; }


        /// <summary>
        ///     Gets the current topic subscription as set by
        ///     <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Subscribe(string)" />.
        /// </summary>
        List<string> Subscription { get; }


        /// <summary>
        ///     Update the topic subscription.
        ///
        ///     Any previous subscription will be
        ///     unassigned and unsubscribed first.
        /// </summary>
        /// <param name="topics">
        ///     The topics to subscribe to.
        ///     A regex can be specified to subscribe to 
        ///     the set of all matching topics (which is
        ///     updated as topics are added / removed from
        ///     the cluster). A regex must be front
        ///     anchored to be recognized as a regex.
        ///     e.g. ^myregex
        /// </param>
        /// <remarks>
        ///     The topic subscription set denotes the
        ///     desired set of topics to consume from.
        ///     This set is provided to the consumer
        ///     group leader (one of the group
        ///     members) which uses the configured
        ///     partition.assignment.strategy to 
        ///     allocate partitions of topics in the
        ///     subscription set to the consumers in
        ///     the group.
        /// </remarks>
        void Subscribe(IEnumerable<string> topics);


        /// <summary>
        ///     Sets the subscription set to a single
        ///     topic.
        ///
        ///     Any previous subscription will be
        ///     unassigned and unsubscribed first.
        /// </summary>
        /// <param name="topic">
        ///     The topic to subscribe to.
        ///     A regex can be specified to subscribe to 
        ///     the set of all matching topics (which is
        ///     updated as topics are added / removed from
        ///     the cluster). A regex must be front
        ///     anchored to be recognized as a regex.
        ///     e.g. ^myregex
        /// </param>
        void Subscribe(string topic);
        

        /// <summary>
        ///     Unsubscribe from the current subscription
        ///     set.
        /// </summary>
        void Unsubscribe();


        /// <summary>
        ///     Sets the current set of assigned partitions
        ///     (the set of partitions the consumer will consume
        ///     from) to a single <paramref name="partition" />. 
        ///
        ///     Note: The newly specified set is the complete
        ///     set of partitions to consume from. If the
        ///     consumer is already assigned to a set of
        ///     partitions, the previous set will be replaced.
        /// </summary>
        /// <param name="partition">
        ///     The partition to consume from.
        ///     Consumption will resume from the last committed
        ///     offset, or according to the 'auto.offset.reset'
        ///     configuration parameter if no offsets have been
        ///     committed yet.
        /// </param>
        void Assign(TopicPartition partition);


        /// <summary>
        ///     Sets the current set of assigned partitions
        ///     (the set of partitions the consumer will consume
        ///     from) to a single <paramref name="partition" />. 
        ///
        ///     Note: The newly specified set is the complete
        ///     set of partitions to consume from. If the
        ///     consumer is already assigned to a set of
        ///     partitions, the previous set will be replaced.
        /// </summary>
        /// <param name="partition">
        ///     The partition to consume from.
        ///     If an offset value of Offset.Unset (-1001) is
        ///     specified, consumption will resume from the last
        ///     committed offset, or according to the
        ///     'auto.offset.reset' configuration parameter if
        ///     no offsets have been committed yet.
        /// </param>
        void Assign(TopicPartitionOffset partition);


        /// <summary>
        ///     Sets the current set of assigned partitions
        ///     (the set of partitions the consumer will consume
        ///     from) to <paramref name="partitions" />. 
        ///
        ///     Note: The newly specified set is the complete
        ///     set of partitions to consume from. If the
        ///     consumer is already assigned to a set of
        ///     partitions, the previous set will be replaced.
        /// </summary>
        /// <param name="partitions">
        ///     The set of partitions to consume from.
        ///     If an offset value of Offset.Unset (-1001) is
        ///     specified for a partition, consumption will
        ///     resume from the last committed offset on that
        ///     partition, or according to the
        ///     'auto.offset.reset' configuration parameter if
        ///     no offsets have been committed yet.
        /// </param>
        void Assign(IEnumerable<TopicPartitionOffset> partitions);


        /// <summary>
        ///     Sets the current set of assigned partitions
        ///     (the set of partitions the consumer will consume
        ///     from) to <paramref name="partitions" />. 
        ///
        ///     Note: The newly specified set is the complete
        ///     set of partitions to consume from. If the
        ///     consumer is already assigned to a set of
        ///     partitions, the previous set will be replaced.
        /// </summary>
        /// <param name="partitions">
        ///     The set of partitions to consume from.
        ///     Consumption will resume from the last committed
        ///     offset on each partition, or according to the
        ///     'auto.offset.reset' configuration parameter if
        ///     no offsets have been committed yet.
        /// </param>
        void Assign(IEnumerable<TopicPartition> partitions);


        /// <summary>
        ///     Incrementally add <paramref name="partitions" />
        ///     to the current assignment, starting consumption
        ///     from the specified offsets.
        /// </summary>
        /// <param name="partitions">
        ///     The set of additional partitions to consume from.
        ///     If an offset value of Offset.Unset (-1001) is
        ///     specified for a partition, consumption will
        ///     resume from the last committed offset on that
        ///     partition, or according to the
        ///     'auto.offset.reset' configuration parameter if
        ///     no offsets have been committed yet.
        /// </param>
        void IncrementalAssign(IEnumerable<TopicPartitionOffset> partitions);


        /// <summary>
        ///     Incrementally add <paramref name="partitions" />
        ///     to the current assignment.
        /// </summary>
        /// <param name="partitions">
        ///     The set of additional partitions to consume from.
        ///     Consumption will resume from the last committed
        ///     offset on each partition, or according to the
        ///     'auto.offset.reset' configuration parameter if
        ///     no offsets have been committed yet.
        /// </param>
        void IncrementalAssign(IEnumerable<TopicPartition> partitions);


        /// <summary>
        ///     Incrementally remove <paramref name="partitions" />
        ///     to the current assignment.
        /// </summary>
        /// <param name="partitions">
        ///     The set of partitions to remove from the current
        ///     assignment.
        /// </param>
        void IncrementalUnassign(IEnumerable<TopicPartition> partitions);


        /// <summary>
        ///     Remove the current set of assigned partitions
        ///     and stop consumption.
        /// </summary>
        void Unassign();


        /// <summary>
        ///     Store offsets for a single partition based on
        ///     the topic/partition/offset of a consume result.
        /// 
        ///     The offset will be committed according to
        ///     `auto.commit.interval.ms` (and
        ///     `enable.auto.commit`) or manual offset-less
        ///     commit().
        /// </summary>
        /// <remarks>
        ///     `enable.auto.offset.store` must be set to
        ///     "false" when using this API.
        /// </remarks>
        /// <param name="result">
        ///     A consume result used to determine
        ///     the offset to store and topic/partition.
        /// </param>
        /// <returns>
        ///     Current stored offset or a partition
        ///     specific error.
        /// </returns>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        /// <exception cref="Confluent.Kafka.TopicPartitionOffsetException">
        ///     Thrown if result is in error.
        /// </exception>
        void StoreOffset(ConsumeResult<TKey, TValue> result);


        /// <summary>
        ///     Store offsets for a single partition.
        /// 
        ///     The offset will be committed (written) to the
        ///     offset store according to `auto.commit.interval.ms`
        ///     or manual offset-less commit(). Calling
        ///     this method in itself does not commit offsets,
        ///     only store them for future commit.
        /// </summary>
        /// <remarks>
        ///     `enable.auto.offset.store` must be set to
        ///     "false" when using this API.
        /// </remarks>
        /// <param name="offset">
        ///     The offset to be committed.
        /// </param>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        void StoreOffset(TopicPartitionOffset offset);


        /// <summary>
        ///     Commit all offsets for the current assignment.
        /// </summary>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        /// <exception cref="Confluent.Kafka.TopicPartitionOffsetException">
        ///     Thrown if any of the constituent results is in
        ///     error. The entire result (which may contain
        ///     constituent results that are not in error) is
        ///     available via the <see cref="Confluent.Kafka.TopicPartitionOffsetException.Results" />
        ///     property of the exception.
        /// </exception>
        List<TopicPartitionOffset> Commit();


        /// <summary>
        ///     Commit an explicit list of offsets.
        /// </summary>
        /// <param name="offsets">
        ///     The topic/partition offsets to commit.
        /// </param>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        /// <exception cref="Confluent.Kafka.TopicPartitionOffsetException">
        ///     Thrown if any of the constituent results is in
        ///     error. The entire result (which may contain
        ///     constituent results that are not in error) is
        ///     available via the <see cref="Confluent.Kafka.TopicPartitionOffsetException.Results" />
        ///     property of the exception.
        /// </exception>
        void Commit(IEnumerable<TopicPartitionOffset> offsets);


        /// <summary>
        ///     Commits an offset based on the
        ///     topic/partition/offset of a ConsumeResult.
        /// </summary>
        /// <param name="result">
        ///     The ConsumeResult instance used
        ///     to determine the committed offset.
        /// </param>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        /// <exception cref="Confluent.Kafka.TopicPartitionOffsetException">
        ///     Thrown if the result is in error.
        /// </exception>
        /// <remarks>
        ///     A consumer at position N has consumed
        ///     messages with offsets up to N-1 and will
        ///     next receive the message with offset N.
        ///     Hence, this method commits an offset of
        ///     <paramref name="result" />.Offset + 1.
        /// </remarks>
        void Commit(ConsumeResult<TKey, TValue> result);


        /// <summary>
        ///     Seek to <parmref name="offset"/> on the
        ///     specified topic partition which is either
        ///     an absolute or logical offset. This must
        ///     only be done for partitions that are 
        ///     currently being consumed (i.e., have been
        ///     Assign()ed). To set the start offset for 
        ///     not-yet-consumed partitions you should use the 
        ///     Assign method instead.
        /// </summary>
        /// <param name="tpo">
        ///     The topic partition to seek
        ///     on and the offset to seek to.
        /// </param>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        void Seek(TopicPartitionOffset tpo);


        /// <summary>
        ///     Pause consumption for the provided list
        ///     of partitions.
        /// </summary>
        /// <param name="partitions">
        ///     The partitions to pause consumption of.
        /// </param>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        /// <exception cref="Confluent.Kafka.TopicPartitionException">
        ///     Per partition success or error.
        /// </exception>
        void Pause(IEnumerable<TopicPartition> partitions);


        /// <summary>
        ///     Resume consumption for the provided list of partitions.
        /// </summary>
        /// <param name="partitions">
        ///     The partitions to resume consumption of.
        /// </param>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        /// <exception cref="Confluent.Kafka.TopicPartitionException">
        ///     Per partition success or error.
        /// </exception>
        void Resume(IEnumerable<TopicPartition> partitions);



        /// <summary>
        ///     Retrieve current committed offsets for the
        ///     current assignment.
        ///
        ///     The offset field of each requested partition
        ///     will be set to the offset of the last consumed
        ///     message, or Offset.Unset in case there was no
        ///     previous message, or, alternately a partition
        ///     specific error may also be returned.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum period of time the call
        ///     may block.
        /// </param>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        /// <exception cref="Confluent.Kafka.TopicPartitionOffsetException">
        ///     Thrown if any of the constituent results is in
        ///     error. The entire result (which may contain
        ///     constituent results that are not in error) is
        ///     available via the
        ///     <see cref="Confluent.Kafka.TopicPartitionOffsetException.Results" />
        ///     property of the exception.
        /// </exception>
        List<TopicPartitionOffset> Committed(TimeSpan timeout);


        /// <summary>
        ///     Retrieve current committed offsets for the
        ///     specified topic partitions.
        ///
        ///     The offset field of each requested partition
        ///     will be set to the offset of the last consumed
        ///     message, or Offset.Unset in case there was no
        ///     previous message, or, alternately a partition
        ///     specific error may also be returned.
        /// </summary>
        /// <param name="partitions">
        ///     the partitions to get the committed
        ///     offsets for.
        /// </param>
        /// <param name="timeout">
        ///     The maximum period of time the call
        ///     may block.
        /// </param>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        /// <exception cref="Confluent.Kafka.TopicPartitionOffsetException">
        ///     Thrown if any of the constituent results is in
        ///     error. The entire result (which may contain
        ///     constituent results that are not in error) is
        ///     available via the
        ///     <see cref="Confluent.Kafka.TopicPartitionOffsetException.Results" />
        ///     property of the exception.
        /// </exception>
        List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout);


        /// <summary>
        ///     Gets the current position (offset) for the
        ///     specified topic / partition.
        ///
        ///     The offset field of each requested partition
        ///     will be set to the offset of the last consumed
        ///     message + 1, or Offset.Unset in case there was
        ///     no previous message consumed by this consumer.
        /// </summary>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        Offset Position(TopicPartition partition);


        /// <summary>
        ///     Look up the offsets for the given partitions
        ///     by timestamp. The returned offset for each
        ///     partition is the earliest offset for which
        ///     the timestamp is greater than or equal to
        ///     the given timestamp. If the provided
        ///     timestamp exceeds that of the last message
        ///     in the partition, a value of Offset.End (-1)
        ///     will be returned.
        /// </summary>
        /// <remarks>
        ///     The consumer does not need to be assigned to
        ///     the requested partitions.
        /// </remarks>
        /// <param name="timestampsToSearch">
        ///     The mapping from partition
        ///     to the timestamp to look up.
        /// </param>
        /// <param name="timeout">
        ///     The maximum period of time the
        ///     call may block.
        /// </param>
        /// <returns>
        ///     A mapping from partition to the
        ///     timestamp and offset of the first message with
        ///     timestamp greater than or equal to the target
        ///     timestamp.
        /// </returns>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown
        ///     if the operation fails.
        /// </exception>
        /// <exception cref="Confluent.Kafka.TopicPartitionOffsetException">
        ///     Thrown if any of the constituent results is
        ///     in error. The entire result (which may contain
        ///     constituent results that are not in error) is
        ///     available via the
        ///     <see cref="Confluent.Kafka.TopicPartitionOffsetException.Results" />
        ///     property of the exception.
        /// </exception>
        List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout);


        /// <summary>
        ///     Get the last cached low (oldest available /
        ///     beginning) and high (newest/end) offsets for
        ///     a topic/partition. Does not block.
        /// </summary>
        /// <remarks>
        ///     The low offset is updated periodically (if
        ///     statistics.interval.ms is set) while the
        ///     high offset is updated on each fetched
        ///     message set from the broker. If there is no
        ///     cached offset (either low or high, or both)
        ///     then Offset.Unset will be returned for the
        ///     respective offset.
        /// </remarks>
        /// <param name="topicPartition">
        ///     The topic partition of interest.
        /// </param>
        /// <returns>
        ///     The requested WatermarkOffsets
        ///     (see that class for additional documentation).
        /// </returns>
        WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition);


        /// <summary>
        ///     Query the Kafka cluster for low (oldest
        ///     available/beginning) and high (newest/end)
        ///     offsets for the specified topic/partition.
        ///     This is a blocking call - always contacts
        ///     the cluster for the required information.
        /// </summary>
        /// <param name="topicPartition">
        ///     The topic/partition of interest.
        /// </param>
        /// <param name="timeout">
        ///     The maximum period of time
        ///     the call may block.
        /// </param>
        /// <returns>
        ///     The requested WatermarkOffsets (see
        ///     that class for additional documentation).
        /// </returns>
        WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout);


        /// <summary>
        ///     Commits offsets (if auto commit is enabled),
        ///     alerts the group coordinator
        ///     that the consumer is exiting the group then
        ///     releases all resources used by this consumer.
        ///     You should call <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Close" />
        ///     instead of <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Dispose()" />
        ///     (or just before) to ensure a timely consumer
        ///     group rebalance. If you do not call
        ///     <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Close" />
        ///     or <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Unsubscribe" />,
        ///     the group will rebalance after a timeout
        ///     specified by the group's `session.timeout.ms`.
        ///     Note: the partition assignment and partitions
        ///     revoked handlers may be called as a side-effect
        ///     of calling this method.
        /// </summary>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the operation fails.
        /// </exception>
        void Close();


        /// <summary>
        ///     The current consumer group metadata associated with this consumer,
        ///     or null if a GroupId has not been specified for the consumer.
        ///     This metadata object should be passed to the transactional producer's
        ///     <see cref="IProducer{K,V}.SendOffsetsToTransaction(IEnumerable{TopicPartitionOffset},IConsumerGroupMetadata,TimeSpan)"/>
        ///     method.
        /// </summary>
        IConsumerGroupMetadata ConsumerGroupMetadata { get; }
    }
}
