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
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Defines a high-level Apache Kafka consumer (with key and 
    ///     value deserialization).
    /// </summary>
    public interface IConsumer<TKey, TValue> : IClient
    {
        /// <summary>
        ///     Gets the (dynamic) group member id of this consumer (as set by the broker).
        /// </summary>
        string MemberId { get; }

        /// <summary>
        ///     Poll for new messages / events. Blocks until a consume result
        ///     is available or the timeout period has elapsed.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
        /// </param>
        /// <returns>
        ///     The consume result.
        /// </returns>
        /// <remarks>
        ///     OnPartitionsAssigned/Revoked and OnOffsetsCommitted events
        ///     may be invoked as a side-effect of calling this method (on 
        ///     the same thread).
        /// </remarks>
        ConsumeResult<TKey, TValue> Consume(TimeSpan timeout);

        /// <summary>
        ///     Poll for new messages / events. Blocks until a consume result
        ///     is available or the operation has been cancelled.
        /// </summary>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used to cancel this operation.
        /// </param>
        /// <returns>
        ///     The consume result.
        /// </returns>
        /// <remarks>
        ///     OnPartitionsAssigned/Revoked and OnOffsetsCommitted events
        ///     may be invoked as a side-effect of calling this method (on 
        ///     the same thread).
        /// </remarks>
        ConsumeResult<TKey, TValue> Consume(CancellationToken cancellationToken);

        /// <summary>
        ///     Poll for new messages / consumer events. Blocks until a consume 
        ///     result is available or the timeout period has elapsed.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
        /// </param>
        /// <returns>
        ///     The consume result.
        /// </returns>
        /// <remarks>
        ///     OnPartitionsAssigned/Revoked and OnOffsetsCommitted events
        ///     may be invoked as a side-effect of calling this method (on 
        ///     the same thread).
        /// </remarks>
        Task<ConsumeResult<TKey, TValue>> ConsumeAsync(TimeSpan timeout);

        /// <summary>
        ///     Poll for new messages / consumer events. Blocks until a consume
        ///     result is available or the operation has been cancelled.
        /// </summary>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used to cancel this operation.
        /// </param>
        /// <returns>
        ///     The consume result.
        /// </returns>
        /// <remarks>
        ///     OnPartitionsAssigned/Revoked and OnOffsetsCommitted events
        ///     may be invoked as a side-effect of calling this method (on 
        ///     the same thread).
        /// </remarks>
        Task<ConsumeResult<TKey, TValue>> ConsumeAsync(CancellationToken cancellationToken);

        /// <summary>
        ///     Raised when a new partition assignment is received.
        /// 
        ///     You must call the <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Assign(IEnumerable{TopicPartitionOffset})" />
        ///     method (or other overload) if you specify this handler. If no handler is provided,
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Assign(IEnumerable{TopicPartition})" />
        ///     will be called automatically.
        /// </summary>
        /// <remarks>
        /// 
        ///     Executes as a side-effect of
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Consume(CancellationToken)" />
        ///     (on the same thread).
        /// </remarks>
        event EventHandler<List<TopicPartition>> OnPartitionAssignmentReceived;

        /// <summary>
        ///     Raised when a partition assignment is revoked.
        /// 
        ///     You must the <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Assign(IEnumerable{TopicPartitionOffset})" />
        ///     method (or other overload) if you specify this handler. If no handler is provided,
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Unassign" /> will be called automatically.
        /// </summary>
        /// <remarks>
        ///     Executes as a side-effect of
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Consume(CancellationToken)" />
        ///     and <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Close()" />
        ///     (on the same thread).
        /// </remarks>
        event EventHandler<List<TopicPartition>> OnPartitionAssignmentRevoked;

        /// <summary>
        ///     Raised to report the result of (automatic) offset commits.
        ///     Not raised as a result of the use of the Commit method.
        /// </summary>
        /// <remarks>
        ///     Executes as a side-effect of
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Consume(CancellationToken)" />
        ///     (on the same thread).
        /// </remarks>
        event EventHandler<CommittedOffsets> OnOffsetsCommitted;

        /// <summary>
        ///     Gets the current partition assignment as set by
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Assign(TopicPartition)" />.
        /// </summary>
        List<TopicPartition> Assignment { get; }

        /// <summary>
        ///     Gets the current partition subscription as set by
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Subscribe(string)" />.
        /// </summary>
        List<string> Subscription { get; }

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
        /// <param name="topics">
        ///     The topics to subscribe to.
        /// </param>
        void Subscribe(IEnumerable<string> topics);

        /// <summary>
        ///     Update the subscription set to a single topic.
        ///
        ///     Any previous subscription will be unassigned and unsubscribed first.
        /// </summary>
        /// <param name="topic">
        ///     The topic to subscribe to.
        /// </param>
        void Subscribe(string topic);
        
        /// <summary>
        ///     Unsubscribe from the current subscription set.
        /// </summary>
        void Unsubscribe();

        /// <summary>
        ///     Update the assignment set to a single <paramref name="partition" />.
        ///
        ///     The assignment set is the complete set of partitions to consume
        ///     from and will replace any previous assignment.
        /// </summary>
        /// <param name="partition">
        ///     The partition to consume from. Consumption will resume from the last
        ///     committed offset, or according to the 'auto.offset.reset' configuration
        ///     parameter if no offsets have been committed yet.
        /// </param>
        void Assign(TopicPartition partition);

        /// <summary>
        ///     Update the assignment set to a single <paramref name="partition" />.
        ///
        ///     The assignment set is the complete set of partitions to consume
        ///     from and will replace any previous assignment.
        /// </summary>
        /// <param name="partition">
        ///     The partition to consume from. If an offset value of Offset.Invalid
        ///     (-1001) is specified, consumption will resume from the last committed
        ///     offset, or according to the 'auto.offset.reset' configuration parameter
        ///     if no offsets have been committed yet.
        /// </param>
        void Assign(TopicPartitionOffset partition);

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
        ///     Store offsets for a single partition based on the topic/partition/offset
        ///     of a consume result.
        /// 
        ///     The offset will be committed (written) to the offset store according
        ///     to `auto.commit.interval.ms` or manual offset-less commit().
        /// </summary>
        /// <remarks>
        ///     `enable.auto.offset.store` must be set to "false" when using this API.
        /// </remarks>
        /// <param name="result">
        ///     A consume result used to determine the offset to store and topic/partition.
        /// </param>
        /// <returns>
        ///     Current stored offset or a partition specific error.
        /// </returns>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        /// <exception cref="Confluent.Kafka.TopicPartitionOffsetException">
        ///     Thrown if result is in error.
        /// </exception>
        TopicPartitionOffset StoreOffset(ConsumeResult<TKey, TValue> result);

        /// <summary>
        ///     Store offsets for one or more partitions.
        /// 
        ///     The offset will be committed (written) to the offset store according
        ///     to `auto.commit.interval.ms` or manual offset-less commit().
        /// </summary>
        /// <remarks>
        ///     `enable.auto.offset.store` must be set to "false" when using this API.
        /// </remarks>
        /// <param name="offsets">
        ///     List of offsets to be commited.
        /// </param>
        /// <returns>
        ///     For each topic/partition returns current stored offset
        ///     or a partition specific error.
        /// </returns>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        /// <exception cref="Confluent.Kafka.TopicPartitionOffsetException">
        ///     Thrown if any of the constituent results is in error. The entire result
        ///     (which may contain constituent results that are not in error) is available
        ///     via the <see cref="Confluent.Kafka.TopicPartitionOffsetException.Results" />
        ///     property of the exception.
        /// </exception>
        List<TopicPartitionOffset> StoreOffsets(IEnumerable<TopicPartitionOffset> offsets);

        /// <summary>
        ///     Commit all offsets for the current assignment.
        /// </summary>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used to cancel this operation
        ///     (currently ignored).
        /// </param>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        /// <exception cref="Confluent.Kafka.TopicPartitionOffsetException">
        ///     Thrown if any of the constituent results is in error. The entire result
        ///     (which may contain constituent results that are not in error) is available
        ///     via the <see cref="Confluent.Kafka.TopicPartitionOffsetException.Results" />
        ///     property of the exception.
        /// </exception>
        Task<List<TopicPartitionOffset>> CommitAsync(CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        ///     Commits an offset based on the topic/partition/offset of a ConsumeResult.
        ///     The next message to be read will be that following <paramref name="result" />.
        /// </summary>
        /// <param name="result">
        ///     The ConsumeResult instance used to determine the committed offset.
        /// </param>
        /// <remarks>
        ///     A consumer which has position N has consumed messages with offsets 0 through N-1 
        ///     and will next receive the message with offset N. Hence, this method commits an 
        ///     offset of <paramref name="result" />.Offset + 1.
        /// </remarks>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used to cancel this operation
        ///     (currently ignored).
        /// </param>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        /// <exception cref="Confluent.Kafka.TopicPartitionOffsetException">
        ///     Thrown if the result is in error.
        /// </exception>
        Task<List<TopicPartitionOffset>> CommitAsync(ConsumeResult<TKey, TValue> result, CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        ///     Commit an explicit list of offsets.
        /// </summary>
        /// <remarks>
        ///     Note: A consumer which has position N has consumed messages with offsets 0 through N-1 
        ///     and will next receive the message with offset N.
        /// </remarks>
        /// <param name="offsets">
        ///     The topic/partition offsets to commit.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used to cancel this operation
        ///     (currently ignored).
        /// </param>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        /// <exception cref="Confluent.Kafka.TopicPartitionOffsetException">
        ///     Thrown if any of the constituent results is in error. The entire result
        ///     (which may contain constituent results that are not in error) is available
        ///     via the <see cref="Confluent.Kafka.TopicPartitionOffsetException.Results" />
        ///     property of the exception.
        /// </exception>
        Task<List<TopicPartitionOffset>> CommitAsync(IEnumerable<TopicPartitionOffset> offsets, CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        ///     Seek to <parmref name="offset"/> on the specified topic/partition which is either
        ///     an absolute or logical offset. This must only be done for partitions that are 
        ///     currently being consumed (i.e., have been Assign()ed). To set the start offset for 
        ///     not-yet-consumed partitions you should use the 
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Assign(TopicPartitionOffset)" /> 
        ///     method (or other overload) instead.
        /// </summary>
        /// <param name="tpo">
        ///     The topic/partition to seek on and the offset to seek to.
        /// </param>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        void Seek(TopicPartitionOffset tpo);

        /// <summary>
        ///     Pause consumption for the provided list of partitions.
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
        ///     Retrieve current committed offsets for the specified topic/partitions.
        ///
        ///     The offset field of each requested partition will be set to the offset
        ///     of the last consumed message, or Offset.Invalid in case there was
        ///     no previous message, or, alternately a partition specific error may also
        ///     be returned.
        /// </summary>
        /// <param name="partitions">
        ///     the partitions to get the committed offsets for.
        /// </param>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used to cancel this operation
        ///     (currently ignored).
        /// </param>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        /// <exception cref="Confluent.Kafka.TopicPartitionOffsetException">
        ///     Thrown if any of the constituent results is in error. The entire result
        ///     (which may contain constituent results that are not in error) is available
        ///     via the <see cref="Confluent.Kafka.TopicPartitionOffsetException.Results" />
        ///     property of the exception.
        /// </exception>
        Task<List<TopicPartitionOffset>> CommittedAsync(IEnumerable<TopicPartition> partitions, TimeSpan timeout, CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        ///     Gets the current positions (offsets) for the specified topics + partitions.
        ///
        ///     The offset field of each requested partition will be set to the offset
        ///     of the last consumed message + 1, or Offset.Invalid in case there was
        ///     no previous message consumed by this consumer.
        /// </summary>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        /// <exception cref="Confluent.Kafka.TopicPartitionOffsetException">
        ///     Thrown if any of the constituent results is in error. The entire result
        ///     (which may contain constituent results that are not in error) is available
        ///     via the <see cref="Confluent.Kafka.TopicPartitionOffsetException.Results" />
        ///     property of the exception.
        /// </exception>
        Task<List<TopicPartitionOffset>> PositionAsync(IEnumerable<TopicPartition> partitions);

        /// <summary>
        ///     Look up the offsets for the given partitions by timestamp. The returned
        ///     offset for each partition is the earliest offset whose timestamp is greater
        ///     than or equal to the given timestamp in the corresponding partition.
        /// </summary>
        /// <remarks>
        ///     This is a blocking call. It will block for the timeout period if the 
        ///     partition does not exist. 
        ///
        ///     The consumer does not need to be assigned to the requested partitions.
        /// </remarks>
        /// <param name="timestampsToSearch">
        ///     The mapping from partition to the timestamp to look up.
        /// </param>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used to cancel this operation
        ///     (currently ignored).
        /// </param>
        /// <returns>
        ///     A mapping from partition to the timestamp and offset of the first message with
        ///     timestamp greater than or equal to the target timestamp.
        /// </returns>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the operation fails.
        /// </exception>
        /// <exception cref="Confluent.Kafka.TopicPartitionOffsetException">
        ///     Thrown if any of the constituent results is in error. The entire result
        ///     (which may contain constituent results that are not in error) is available
        ///     via the <see cref="Confluent.Kafka.TopicPartitionOffsetException.Results" />
        ///     property of the exception.
        /// </exception>
        Task<List<TopicPartitionOffset>> OffsetsForTimesAsync(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout, CancellationToken cancellationToken = default(CancellationToken));
    }
}
