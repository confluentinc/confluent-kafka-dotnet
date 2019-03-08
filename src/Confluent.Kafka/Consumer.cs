// Copyright 2016-2018 Confluent Inc., 2015-2016 Andreas Heider
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
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Serdes;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Implements a high-level Apache Kafka consumer with
    ///     deserialization capability.
    /// </summary>
    public class Consumer<TKey, TValue> : IConsumer<TKey, TValue>, IClient
    {
        internal class Config
        {
            internal IEnumerable<KeyValuePair<string, string>> config;
            internal Action<Error> errorHandler;
            internal Action<LogMessage> logHandler;
            internal Action<string> statisticsHandler;
            internal Action<CommittedOffsets> offsetsCommittedHandler;
            internal Action<RebalanceEvent> rebalanceHandler;
        }

        private IDeserializer<TKey> keyDeserializer;
        private IDeserializer<TValue> valueDeserializer;
        private IAsyncDeserializer<TKey> asyncKeyDeserializer;
        private IAsyncDeserializer<TValue> asyncValueDeserializer;

        private Dictionary<Type, object> defaultDeserializers = new Dictionary<Type, object>
        {
            { typeof(Null), Deserializers.Null },
            { typeof(Ignore), Deserializers.Ignore },
            { typeof(int), Deserializers.Int32 },
            { typeof(long), Deserializers.Int64 },
            { typeof(string), Deserializers.Utf8 },
            { typeof(float), Deserializers.Single },
            { typeof(double), Deserializers.Double },
            { typeof(byte[]), Deserializers.ByteArray }
        };

        private int cancellationDelayMaxMs;

        private bool disposeHasBeenCalled = false;
        private object disposeHasBeenCalledLockObj = new object();

        /// <summary>
        ///     keeps track of whether or not assign has been called during
        ///     invocation of a rebalance callback event.
        /// </summary>
        private int assignCallCount = 0;
        private object assignCallCountLockObj = new object();

        private bool enableHeaderMarshaling = true;
        private bool enableTimestampMarshaling = true;
        private bool enableTopicNameMarshaling = true;

        private SafeKafkaHandle kafkaHandle;

        private Action<Error> errorHandler;
        private Librdkafka.ErrorDelegate errorCallbackDelegate;
        private void ErrorCallback(IntPtr rk, ErrorCode err, string reason, IntPtr opaque)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (kafkaHandle.IsClosed) { return; }
            errorHandler?.Invoke(kafkaHandle.CreatePossiblyFatalError(err, reason));
        }

        private Action<string> statisticsHandler;
        private Librdkafka.StatsDelegate statisticsCallbackDelegate;
        private int StatisticsCallback(IntPtr rk, IntPtr json, UIntPtr json_len, IntPtr opaque)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (kafkaHandle.IsClosed) { return 0; }
            statisticsHandler?.Invoke(Util.Marshal.PtrToStringUTF8(json));
            return 0; // instruct librdkafka to immediately free the json ptr.
        }

        private Action<LogMessage> logHandler;
        private object loggerLockObj = new object();
        private Librdkafka.LogDelegate logCallbackDelegate;
        private void LogCallback(IntPtr rk, SyslogLevel level, string fac, string buf)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            // Note: kafkaHandle can be null if the callback is during construction (in that case the delegate should be called).
            if (kafkaHandle != null && kafkaHandle.IsClosed) { return; }
            logHandler?.Invoke(new LogMessage(Util.Marshal.PtrToStringUTF8(Librdkafka.name(rk)), level, fac, buf));
        }

        private Action<RebalanceEvent> rebalanceHandler;
        private Librdkafka.RebalanceDelegate rebalanceDelegate;
        private void RebalanceCallback(
            IntPtr rk,
            ErrorCode err,
            IntPtr partitions,
            IntPtr opaque)
        {
            var partitionAssignment = SafeKafkaHandle.GetTopicPartitionOffsetErrorList(partitions).Select(p => p.TopicPartition).ToList();

            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (kafkaHandle.IsClosed)
            { 
                // The RebalanceCallback should never be invoked as a side effect of Dispose.
                // If for some reason flow of execution gets here, something is badly wrong. 
                // (and we have a closed librdkafka handle that is expecting an assign call...)
                throw new Exception("Unexpected rebalance callback on disposed kafkaHandle");
            }

            // Note: The contract with librdkafka requires the application to acknowledge rebalances by calling Assign.
            // To make the API less error prone, this is done automatically by the C# binding if required - the number
            // of times assign is called by user code is tracked, and if this is zero, then assign is called automatically.

            if (err == ErrorCode.Local_AssignPartitions)
            {
                if (rebalanceHandler != null)
                {
                    lock (assignCallCountLockObj) { assignCallCount = 0; }
                    rebalanceHandler(new RebalanceEvent(partitionAssignment, true));
                    lock (assignCallCountLockObj)
                    {
                        if (assignCallCount > 0) { return; }
                    }
                }
                Assign(partitionAssignment.Select(p => new TopicPartitionOffset(p, Offset.Invalid)));
            }
            else if (err == ErrorCode.Local_RevokePartitions)
            {
                if (rebalanceHandler != null)
                {
                    lock (assignCallCountLockObj) { assignCallCount = 0; }
                    rebalanceHandler(new RebalanceEvent(partitionAssignment, false));
                    lock (assignCallCountLockObj)
                    {
                        if (assignCallCount > 0) { return; }
                    }
                }
                Unassign();
            }
            else
            {
                throw new KafkaException(kafkaHandle.CreatePossiblyFatalError(err, null));
            }
        }

        private Action<CommittedOffsets> offsetsCommittedHandler;
        private Librdkafka.CommitDelegate commitDelegate;
        private void CommitCallback(
            IntPtr rk,
            ErrorCode err,
            IntPtr offsets,
            IntPtr opaque)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (kafkaHandle.IsClosed) { return; }

            offsetsCommittedHandler?.Invoke(new CommittedOffsets(
                SafeKafkaHandle.GetTopicPartitionOffsetErrorList(offsets),
                kafkaHandle.CreatePossiblyFatalError(err, null)
            ));
        }

        private static byte[] KeyAsByteArray(rd_kafka_message msg)
        {
            byte[] keyAsByteArray = null;
            if (msg.key != IntPtr.Zero)
            {
                keyAsByteArray = new byte[(int) msg.key_len];
                Marshal.Copy(msg.key, keyAsByteArray, 0, (int) msg.key_len);
            }
            return keyAsByteArray;
        }

        private static byte[] ValueAsByteArray(rd_kafka_message msg)
        {
            byte[] valAsByteArray = null;
            if (msg.val != IntPtr.Zero)
            {
                valAsByteArray = new byte[(int) msg.len];
                Marshal.Copy(msg.val, valAsByteArray, 0, (int) msg.len);
            }
            return valAsByteArray;
        }


        /// <summary>
        ///     Gets the current partition assignment as set by
        ///     <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Assign(TopicPartition)" />
        ///     or implicitly.
        /// </summary>
        public List<TopicPartition> Assignment
            => kafkaHandle.GetAssignment();


        /// <summary>
        ///     Gets the current topic subscription as set by
        ///     <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Subscribe(string)" />.
        /// </summary>
        public List<string> Subscription
            => kafkaHandle.GetSubscription();


        /// <summary>
        ///     Update the topic subscription.
        ///
        ///     Any previous subscription will be unassigned and unsubscribed first.
        /// </summary>
        /// <param name="topics">
        ///     The topics to subscribe to. A regex can be specified to subscribe to 
        ///     the set of all matching topics (which is updated as topics are added
        ///     / removed from the cluster). A regex must be front anchored to be
        ///     recognized as a regex. e.g. ^myregex
        /// </param>
        /// <remarks>
        ///     The topic subscription set denotes the desired set of topics to consume
        ///     from. This set is provided to the consumer group leader (one of the group
        ///     members) which uses the configured partition.assignment.strategy to 
        ///     allocate partitions of topics in the subscription set to the consumers
        ///     in the group.
        /// </remarks>
        public void Subscribe(IEnumerable<string> topics)
        {
            kafkaHandle.Subscribe(topics);
        }


        /// <summary>
        ///     Sets the subscription set to a single topic.
        ///
        ///     Any previous subscription will be unassigned and unsubscribed first.
        /// </summary>
        /// <param name="topic">
        ///     The topic to subscribe to. A regex can be specified to subscribe to 
        ///     the set of all matching topics (which is updated as topics are added
        ///     / removed from the cluster). A regex must be front anchored to be 
        ///     recognized as a regex. e.g. ^myregex
        /// </param>
        public void Subscribe(string topic)
            => Subscribe(new[] { topic });


        /// <summary>
        ///     Unsubscribe from the current subscription set.
        /// </summary>
        public void Unsubscribe()
            => kafkaHandle.Unsubscribe();


        /// <summary>
        ///     Sets the current set of assigned partitions (the set of partitions the
        ///     consumer will consume from) to a single <paramref name="partition" />. 
        ///
        ///     Note: The newly specified set is the complete set of partitions to
        ///     consume from. If the consumer is already assigned to a set of partitions,
        ///     the previous set will be replaced.
        /// </summary>
        /// <param name="partition">
        ///     The partition to consume from. Consumption will resume from the last
        ///     committed offset, or according to the 'auto.offset.reset' configuration
        ///     parameter if no offsets have been committed yet.
        /// </param>
        public void Assign(TopicPartition partition)
            => Assign(new List<TopicPartition> { partition });


        /// <summary>
        ///     Sets the current set of assigned partitions (the set of partitions the
        ///     consumer will consume from) to a single <paramref name="partition" />. 
        ///
        ///     Note: The newly specified set is the complete set of partitions to
        ///     consume from. If the consumer is already assigned to a set of partitions,
        ///     the previous set will be replaced.
        /// </summary>
        /// <param name="partition">
        ///     The partition to consume from. If an offset value of Offset.Invalid
        ///     (-1001) is specified, consumption will resume from the last committed
        ///     offset, or according to the 'auto.offset.reset' configuration parameter
        ///     if no offsets have been committed yet.
        /// </param>
        public void Assign(TopicPartitionOffset partition)
            => Assign(new List<TopicPartitionOffset> { partition });


        /// <summary>
        ///     Sets the current set of assigned partitions (the set of partitions the
        ///     consumer will consume from) to <paramref name="partitions" />. 
        ///
        ///     Note: The newly specified set is the complete set of partitions to
        ///     consume from. If the consumer is already assigned to a set of partitions,
        ///     the previous set will be replaced.
        /// </summary>
        /// <param name="partitions">
        ///     The set of partitions to consume from. If an offset value of
        ///     Offset.Invalid (-1001) is specified for a partition, consumption
        ///     will resume from the last committed offset on that partition, or
        ///     according to the 'auto.offset.reset' configuration parameter if
        ///     no offsets have been committed yet.
        /// </param>
        public void Assign(IEnumerable<TopicPartitionOffset> partitions)
        {
            lock (assignCallCountLockObj) { assignCallCount += 1; }
            kafkaHandle.Assign(partitions.ToList());
        }


        /// <summary>
        ///     Sets the current set of assigned partitions (the set of partitions the
        ///     consumer will consume from) to <paramref name="partitions" />. 
        ///
        ///     Note: The newly specified set is the complete set of partitions to
        ///     consume from. If the consumer is already assigned to a set of partitions,
        ///     the previous set will be replaced.
        /// </summary>
        /// <param name="partitions">
        ///     The set of partitions to consume from. Consumption will resume
        ///     from the last committed offset on each partition, or according
        ///     to the 'auto.offset.reset' configuration parameter if no offsets
        ///     have been committed yet.
        /// </param>
        public void Assign(IEnumerable<TopicPartition> partitions)
        {
            lock (assignCallCountLockObj) { assignCallCount += 1; }
            kafkaHandle.Assign(partitions.Select(p => new TopicPartitionOffset(p, Offset.Invalid)).ToList());
        }


        /// <summary>
        ///     Remove the current set of assigned partitions and stop consumption.
        /// </summary>
        public void Unassign()
        {
            lock (assignCallCountLockObj) { assignCallCount += 1; }
            kafkaHandle.Assign(null);
        }


        /// <summary>
        ///     Store offsets for a single partition based on the topic/partition/offset
        ///     of a consume result.
        /// 
        ///     The offset will be committed according to `auto.commit.interval.ms`
        ///     (and `enable.auto.commit`) or manual offset-less commit().
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
        public void StoreOffset(ConsumeResult<TKey, TValue> result)
            => StoreOffsets(new[] { new TopicPartitionOffset(result.TopicPartition, result.Offset + 1) });


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
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        /// <exception cref="Confluent.Kafka.TopicPartitionOffsetException">
        ///     Thrown if any of the constituent results is in error. The entire result
        ///     (which may contain constituent results that are not in error) is available
        ///     via the <see cref="Confluent.Kafka.TopicPartitionOffsetException.Results" />
        ///     property of the exception.
        /// </exception>
        public void StoreOffsets(IEnumerable<TopicPartitionOffset> offsets)
            => kafkaHandle.StoreOffsets(offsets);


        /// <summary>
        ///     Commit all offsets for the current assignment.
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
        public List<TopicPartitionOffset> Commit()
            // TODO: use a librdkafka queue for this.
            => kafkaHandle.Commit(null);


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
        ///     Thrown if any of the constituent results is in error. The entire result
        ///     (which may contain constituent results that are not in error) is available
        ///     via the <see cref="Confluent.Kafka.TopicPartitionOffsetException.Results" />
        ///     property of the exception.
        /// </exception>
        public void Commit(IEnumerable<TopicPartitionOffset> offsets)
            // TODO: use a librdkafka queue for this.
            => kafkaHandle.Commit(offsets);


        /// <summary>
        ///     Commits an offset based on the topic/partition/offset of a ConsumeResult.
        /// </summary>
        /// <param name="result">
        ///     The ConsumeResult instance used to determine the committed offset.
        /// </param>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        /// <exception cref="Confluent.Kafka.TopicPartitionOffsetException">
        ///     Thrown if the result is in error.
        /// </exception>
        /// <remarks>
        ///     A consumer at position N has consumed messages with offsets up to N-1 
        ///     and will next receive the message with offset N. Hence, this method commits an 
        ///     offset of <paramref name="result" />.Offset + 1.
        /// </remarks>
        public void Commit(ConsumeResult<TKey, TValue> result)
        {
            if (result.Message == null)
            {
                throw new InvalidOperationException("Attempt was made to commit offset corresponding to an empty consume result");
            }

            Commit(new [] { new TopicPartitionOffset(result.TopicPartition, result.Offset + 1) });
        }

        
        /// <summary>
        ///     Seek to <parmref name="offset"/> on the specified topic/partition which is either
        ///     an absolute or logical offset. This must only be done for partitions that are 
        ///     currently being consumed (i.e., have been Assign()ed). To set the start offset for 
        ///     not-yet-consumed partitions you should use the 
        ///     <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Assign(TopicPartitionOffset)" /> 
        ///     method (or other overload) instead.
        /// </summary>
        /// <param name="tpo">
        ///     The topic/partition to seek on and the offset to seek to.
        /// </param>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        public void Seek(TopicPartitionOffset tpo)
            => kafkaHandle.Seek(tpo.Topic, tpo.Partition, tpo.Offset, -1);


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
        public void Pause(IEnumerable<TopicPartition> partitions)
            => kafkaHandle.Pause(partitions);


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
        public void Resume(IEnumerable<TopicPartition> partitions)
            => kafkaHandle.Resume(partitions);


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
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        /// <exception cref="Confluent.Kafka.TopicPartitionOffsetException">
        ///     Thrown if any of the constituent results is in error. The entire result
        ///     (which may contain constituent results that are not in error) is available
        ///     via the <see cref="Confluent.Kafka.TopicPartitionOffsetException.Results" />
        ///     property of the exception.
        /// </exception>
        public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
            // TODO: use a librdkafka queue for this.
            => kafkaHandle.Committed(partitions, (IntPtr)timeout.TotalMillisecondsAsInt());


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
        public List<TopicPartitionOffset> Position(IEnumerable<TopicPartition> partitions)
            // TODO: use a librdkafka queue for this.
            => kafkaHandle.Position(partitions);


        /// <summary>
        ///     Look up the offsets for the given partitions by timestamp. The returned
        ///     offset for each partition is the earliest offset whose timestamp is greater
        ///     than or equal to the given timestamp in the corresponding partition.
        /// </summary>
        /// <remarks>
        ///     The consumer does not need to be assigned to the requested partitions.
        /// </remarks>
        /// <param name="timestampsToSearch">
        ///     The mapping from partition to the timestamp to look up.
        /// </param>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
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
        public List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout)
            // TODO: use a librdkafka queue for this.
            => kafkaHandle.OffsetsForTimes(timestampsToSearch, timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     Get the last cached low (oldest available/beginning) and high (newest/end)
        ///     offsets for a topic/partition. Does not block.
        /// </summary>
        /// <remarks>
        ///     The low offset is updated periodically (if statistics.interval.ms 
        ///     is set) while the high offset is updated on each fetched message set from
        ///     the broker. If there is no cached offset (either low or high, or both) then
        ///     Offset.Invalid will be returned for the respective offset.
        /// </remarks>
        /// <param name="topicPartition">
        ///     The topic/partition of interest.
        /// </param>
        /// <returns>
        ///     The requested WatermarkOffsets (see that class for additional documentation).
        /// </returns>
        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
            => kafkaHandle.GetWatermarkOffsets(topicPartition.Topic, topicPartition.Partition);


        /// <summary>
        ///     Query the Kafka cluster for low (oldest available/beginning) and high (newest/end)
        ///     offsets for the specified topic/partition. This is a blocking call - always contacts
        ///     the cluster for the required information.
        /// </summary>
        /// <param name="topicPartition">
        ///     The topic/partition of interest.
        /// </param>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
        /// </param>
        /// <returns>
        ///     The requested WatermarkOffsets (see that class for additional documentation).
        /// </returns>
        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
            => kafkaHandle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     Gets the (dynamic) group member id of this consumer (as 
        ///     set by the broker).
        /// </summary>
        public string MemberId
            => kafkaHandle.MemberId;


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IClient.AddBrokers(string)" />
        /// </summary>
        public int AddBrokers(string brokers)
            => kafkaHandle.AddBrokers(brokers);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IClient.Name" />
        /// </summary>
        public string Name
            => kafkaHandle.Name;


        /// <summary>
        ///     An opaque reference to the underlying librdkafka client instance.
        ///     This can be used to construct an AdminClient that utilizes the same
        ///     underlying librdkafka client as this Consumer instance.
        /// </summary>
        public Handle Handle
            => new Handle { Owner = this, LibrdkafkaHandle = kafkaHandle };


        /// <summary>
        ///     Commits offsets, alerts the group coodinator that the consumer is
        ///     exiting the group then releases all resources used by this consumer.
        ///     You should call <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Close" />
        ///     instead of <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Dispose()" />
        ///     (or just before) to ensure a timely consumer-group rebalance. If you
        ///     do not call <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Close" />
        ///     or <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Unsubscribe" />,
        ///     the group will rebalance after a timeout specified by the group's 
        ///     `session.timeout.ms`. Note: the partition asignment and partitions
        ///     revoked handlers may be called as a side-effect of
        ///     calling this method.
        /// </summary>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the operation fails.
        /// </exception>
        public void Close()
        {
            // commits offsets and unsubscribes.
            kafkaHandle.ConsumerClose();

            Dispose(true);
            GC.SuppressFinalize(this);
        }


        /// <summary>
        ///     Releases all resources used by this Consumer without
        ///     committing offsets and without alerting the group coordinator
        ///     that the consumer is exiting the group. If you do not call 
        ///     <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Close" /> or
        ///     <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Unsubscribe" />
        ///     prior to Dispose, the group will rebalance after a timeout 
        ///     specified by group's `session.timeout.ms`.
        ///     You should commit offsets / unsubscribe from the group before 
        ///     calling this method (typically by calling 
        ///     <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Close()" />).
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        /// <summary>
        ///     Releases the unmanaged resources used by the
        ///     <see cref="Confluent.Kafka.Consumer{TKey,TValue}" />
        ///     and optionally disposes the managed resources.
        /// </summary>
        /// <param name="disposing">
        ///     true to release both managed and unmanaged resources;
        ///     false to release only unmanaged resources.
        /// </param>
        protected virtual void Dispose(bool disposing)
        {
            // Calling Dispose a second or subsequent time should be a no-op.
            lock (disposeHasBeenCalledLockObj)
            { 
                if (disposeHasBeenCalled) { return; }
                disposeHasBeenCalled = true;
            }

            if (disposing)
            {
                // calls to rd_kafka_destroy may result in callbacks
                // as a side-effect. however the callbacks this class
                // registers with librdkafka ensure that any registered
                // events are not called if the kafkaHandle is closed.
                // this avoids deadlocks in common scenarios.
                kafkaHandle.Dispose();
            }
        }


        internal Consumer(ConsumerBuilder<TKey, TValue> builder)
        {
            var baseConfig = builder.ConstructBaseConfig(this);

            this.statisticsHandler = baseConfig.statisticsHandler;
            this.logHandler = baseConfig.logHandler;
            this.errorHandler = baseConfig.errorHandler;
            this.rebalanceHandler = baseConfig.rebalanceHandler;
            this.offsetsCommittedHandler = baseConfig.offsetsCommittedHandler;

            Librdkafka.Initialize(null);

            var config = Confluent.Kafka.Config.ExtractCancellationDelayMaxMs(baseConfig.config, out this.cancellationDelayMaxMs);

            if (config.FirstOrDefault(prop => string.Equals(prop.Key, "group.id", StringComparison.Ordinal)).Value == null)
            {
                throw new ArgumentException("'group.id' configuration parameter is required and was not specified.");
            }

            var modifiedConfig = config
                .Where(prop => prop.Key != ConfigPropertyNames.Consumer.ConsumeResultFields);

            var enabledFieldsObj = config.FirstOrDefault(prop => prop.Key == ConfigPropertyNames.Consumer.ConsumeResultFields).Value;
            if (enabledFieldsObj != null)
            {
                var fields = enabledFieldsObj.ToString().Replace(" ", "");
                if (fields != "all")
                {
                    this.enableHeaderMarshaling = false;
                    this.enableTimestampMarshaling = false;
                    this.enableTopicNameMarshaling = false;
                    if (fields != "none")
                    {
                        var parts = fields.Split(',');
                        foreach (var part in parts)
                        {
                            switch (part)
                            {
                                case "headers": this.enableHeaderMarshaling = true; break;
                                case "timestamp": this.enableTimestampMarshaling = true; break;
                                case "topic": this.enableTopicNameMarshaling = true; break;
                                default: throw new ArgumentException(
                                    $"Unexpected consume result field name '{part}' in config value '{ConfigPropertyNames.Consumer.ConsumeResultFields}'.");
                            }
                        }
                    }
                }
            }

            var configHandle = SafeConfigHandle.Create();
            modifiedConfig
                .ToList()
                .ForEach((kvp) => {
                    if (kvp.Value == null) throw new ArgumentNullException($"'{kvp.Key}' configuration parameter must not be null.");
                    configHandle.Set(kvp.Key, kvp.Value.ToString());
                });

            // Explicitly keep references to delegates so they are not reclaimed by the GC.
            rebalanceDelegate = RebalanceCallback;
            commitDelegate = CommitCallback;
            errorCallbackDelegate = ErrorCallback;
            logCallbackDelegate = LogCallback;
            statisticsCallbackDelegate = StatisticsCallback;

            IntPtr configPtr = configHandle.DangerousGetHandle();

            Librdkafka.conf_set_rebalance_cb(configPtr, rebalanceDelegate);
            Librdkafka.conf_set_offset_commit_cb(configPtr, commitDelegate);

            Librdkafka.conf_set_error_cb(configPtr, errorCallbackDelegate);
            Librdkafka.conf_set_log_cb(configPtr, logCallbackDelegate);
            Librdkafka.conf_set_stats_cb(configPtr, statisticsCallbackDelegate);

            this.kafkaHandle = SafeKafkaHandle.Create(RdKafkaType.Consumer, configPtr, this);
            configHandle.SetHandleAsInvalid(); // config object is no longer useable.

            var pollSetConsumerError = kafkaHandle.PollSetConsumer();
            if (pollSetConsumerError != ErrorCode.NoError)
            {
                throw new KafkaException(new Error(pollSetConsumerError,
                    $"Failed to redirect the poll queue to consumer_poll queue: {ErrorCodeExtensions.GetReason(pollSetConsumerError)}"));
            }

            // setup key deserializer.
            if (builder.KeyDeserializer == null && builder.AsyncKeyDeserializer == null)
            {
                if (!defaultDeserializers.TryGetValue(typeof(TKey), out object deserializer))
                {
                    throw new InvalidOperationException(
                        $"Key deserializer was not specified and there is no default deserializer defined for type {typeof(TKey).Name}.");
                }
                this.keyDeserializer = (IDeserializer<TKey>)deserializer;
            }
            else if (builder.KeyDeserializer == null && builder.AsyncKeyDeserializer != null)
            {
                this.asyncKeyDeserializer = builder.AsyncKeyDeserializer;
            }
            else if (builder.KeyDeserializer != null && builder.AsyncKeyDeserializer == null)
            {
                this.keyDeserializer = builder.KeyDeserializer;
            }
            else
            {
                // enforced by the builder class.
                throw new InvalidOperationException("FATAL: Both async and sync key deserializers were set.");
            }

            // setup value deserializer.
            if (builder.ValueDeserializer == null && builder.AsyncValueDeserializer == null)
            {
                if (!defaultDeserializers.TryGetValue(typeof(TValue), out object deserializer))
                {
                    throw new InvalidOperationException(
                        $"Value deserializer was not specified and there is no default deserializer defined for type {typeof(TKey).Name}.");
                }
                this.valueDeserializer = (IDeserializer<TValue>)deserializer;
            }
            else if (builder.ValueDeserializer == null && builder.AsyncValueDeserializer != null)
            {
                this.asyncValueDeserializer = builder.AsyncValueDeserializer;
            }
            else if (builder.ValueDeserializer != null && builder.AsyncValueDeserializer == null)
            {
                this.valueDeserializer = builder.ValueDeserializer;
            }
            else
            {
                // enforced by the builder class.
                throw new InvalidOperationException("FATAL: Both async and sync value deserializers were set.");
            }
        }


        private ConsumeResult<K, V> ConsumeImpl<K,V>(
            int millisecondsTimeout,
            IDeserializer<K> keyDeserializer,
            IDeserializer<V> valueDeserializer)
        {
            var msgPtr = kafkaHandle.ConsumerPoll((IntPtr)millisecondsTimeout);
            if (msgPtr == IntPtr.Zero)
            {
                return null;
            }

            try
            {
                var msg = Util.Marshal.PtrToStructure<rd_kafka_message>(msgPtr);

                string topic = null;
                if (this.enableTopicNameMarshaling)
                {
                    if (msg.rkt != IntPtr.Zero)
                    {
                        topic = Util.Marshal.PtrToStringUTF8(Librdkafka.topic_name(msg.rkt));
                    }
                }

                if (msg.err == ErrorCode.Local_PartitionEOF)
                {
                    return new ConsumeResult<K, V>
                    {
                        TopicPartitionOffset = new TopicPartitionOffset(topic, msg.partition, msg.offset),
                        Message = null,
                        IsPartitionEOF = true
                    };
                }

                long timestampUnix = 0;
                IntPtr timestampType = (IntPtr)TimestampType.NotAvailable;
                if (enableTimestampMarshaling)
                {
                    timestampUnix = Librdkafka.message_timestamp(msgPtr, out timestampType);
                }
                var timestamp = new Timestamp(timestampUnix, (TimestampType)timestampType);

                Headers headers = null;
                if (enableHeaderMarshaling)
                {
                    headers = new Headers();
                    Librdkafka.message_headers(msgPtr, out IntPtr hdrsPtr);
                    if (hdrsPtr != IntPtr.Zero)
                    {
                        for (var i=0; ; ++i)
                        {
                            var err = Librdkafka.header_get_all(hdrsPtr, (IntPtr)i, out IntPtr namep, out IntPtr valuep, out IntPtr sizep);
                            if (err != ErrorCode.NoError)
                            {
                                break;
                            }
                            var headerName = Util.Marshal.PtrToStringUTF8(namep);
                            byte[] headerValue = null;
                            if (valuep != IntPtr.Zero)
                            {
                                headerValue = new byte[(int)sizep];
                                Marshal.Copy(valuep, headerValue, 0, (int)sizep);
                            }
                            headers.Add(headerName, headerValue);
                        }
                    }
                }

                if (msg.err != ErrorCode.NoError)
                {
                    throw new ConsumeException(
                        new ConsumeResult<byte[], byte[]>
                        {
                            TopicPartitionOffset = new TopicPartitionOffset(topic, msg.partition, msg.offset),
                            Message = new Message<byte[], byte[]>
                            {
                                Timestamp = timestamp,
                                Headers = headers,
                                Key = KeyAsByteArray(msg),
                                Value = ValueAsByteArray(msg)
                            },
                            IsPartitionEOF = false
                        },
                        kafkaHandle.CreatePossiblyFatalError(msg.err, null));
                }

                K key;
                try
                {
                    unsafe
                    {
                        key = keyDeserializer.Deserialize(
                            msg.key == IntPtr.Zero
                                ? ReadOnlySpan<byte>.Empty
                                : new ReadOnlySpan<byte>(msg.key.ToPointer(), (int)msg.key_len),
                            msg.key == IntPtr.Zero,
                            new SerializationContext(MessageComponentType.Key, topic));
                    }
                }
                catch (Exception exception)
                {
                    throw new ConsumeException(
                        new ConsumeResult<byte[], byte[]>
                        {
                            TopicPartitionOffset = new TopicPartitionOffset(topic, msg.partition, msg.offset),
                            Message = new Message<byte[], byte[]>
                            {
                                Timestamp = timestamp,
                                Headers = headers,
                                Key = KeyAsByteArray(msg),
                                Value = ValueAsByteArray(msg)
                            },
                            IsPartitionEOF = false
                        },
                        new Error(ErrorCode.Local_KeyDeserialization),
                        exception);
                }

                V val;
                try
                {
                    unsafe
                    {
                        val = valueDeserializer.Deserialize(
                            msg.val == IntPtr.Zero
                                ? ReadOnlySpan<byte>.Empty
                                : new ReadOnlySpan<byte>(msg.val.ToPointer(), (int)msg.len),
                            msg.val == IntPtr.Zero,
                            new SerializationContext(MessageComponentType.Value, topic));
                    }
                }
                catch (Exception exception)
                {
                    throw new ConsumeException(
                        new ConsumeResult<byte[], byte[]>
                        {
                            TopicPartitionOffset = new TopicPartitionOffset(topic, msg.partition, msg.offset),
                            Message = new Message<byte[], byte[]>
                            {
                                Timestamp = timestamp,
                                Headers = headers,
                                Key = KeyAsByteArray(msg),
                                Value = ValueAsByteArray(msg)
                            },
                            IsPartitionEOF = false
                        },
                        new Error(ErrorCode.Local_ValueDeserialization),
                        exception);
                }

                return new ConsumeResult<K, V> 
                {
                    TopicPartitionOffset = new TopicPartitionOffset(topic, msg.partition, msg.offset),
                    Message = new Message<K, V>
                    {
                        Timestamp = timestamp,
                        Headers = headers,
                        Key = key,
                        Value = val
                    },
                    IsPartitionEOF = false
                };
            }
            finally
            {
                Librdkafka.message_destroy(msgPtr);
            }
        }


        private ConsumeResult<TKey, TValue> ConsumeViaBytes(int millisecondsTimeout)
        {
            // TODO: add method(s) to ConsumerBase to handle the async case more optimally.
            var rawResult = ConsumeImpl(millisecondsTimeout, Deserializers.ByteArray, Deserializers.ByteArray);
            if (rawResult == null) { return null; }
            if (rawResult.Message == null)
            {
                return new ConsumeResult<TKey, TValue>
                {
                    TopicPartitionOffset = rawResult.TopicPartitionOffset,
                    Message = null,
                    IsPartitionEOF = rawResult.IsPartitionEOF // always true
                };
            }

            TKey key = keyDeserializer != null
                ? keyDeserializer.Deserialize(rawResult.Key, rawResult.Key == null, new SerializationContext(MessageComponentType.Key, rawResult.Topic))
                : asyncKeyDeserializer.DeserializeAsync(new ReadOnlyMemory<byte>(rawResult.Key), rawResult.Key == null, new SerializationContext(MessageComponentType.Key, rawResult.Topic))
                    .ConfigureAwait(continueOnCapturedContext: false)
                    .GetAwaiter()
                    .GetResult();

            TValue val = valueDeserializer != null
                ? valueDeserializer.Deserialize(rawResult.Value, rawResult.Value == null, new SerializationContext(MessageComponentType.Value, rawResult.Topic))
                : asyncValueDeserializer.DeserializeAsync(new ReadOnlyMemory<byte>(rawResult.Value), rawResult == null, new SerializationContext(MessageComponentType.Value, rawResult.Topic))
                    .ConfigureAwait(continueOnCapturedContext: false)
                    .GetAwaiter()
                    .GetResult();

            return new ConsumeResult<TKey, TValue>
            {
                TopicPartitionOffset = rawResult.TopicPartitionOffset,
                Message = rawResult.Message == null ? null : new Message<TKey, TValue>
                {
                    Key = key,
                    Value = val,
                    Headers = rawResult.Headers,
                    Timestamp = rawResult.Timestamp
                },
                IsPartitionEOF = rawResult.IsPartitionEOF
            };
        }


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
        ///     OnPartitionsAssigned/Revoked and OnOffsetsCommitted events may
        ///     be invoked as a side-effect of calling this method (on the same
        ///     thread).
        /// </remarks>
        public ConsumeResult<TKey, TValue> Consume(CancellationToken cancellationToken = default(CancellationToken))
        {
            while (true)
            {
                // Note: An alternative to throwing on cancellation is to return null,
                // but that would be problematic downstream (require null checks).
                cancellationToken.ThrowIfCancellationRequested();
                ConsumeResult<TKey, TValue> result = (keyDeserializer != null && valueDeserializer != null)
                    ? ConsumeImpl<TKey, TValue>(cancellationDelayMaxMs, keyDeserializer, valueDeserializer) // fast path for simple case.
                    : ConsumeViaBytes(cancellationDelayMaxMs);

                if (result == null) { continue; }
                return result;
            }
        }


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
        ///     OnPartitionsAssigned/Revoked and OnOffsetsCommitted events may
        ///     be invoked as a side-effect of calling this method (on the same
        ///     thread).
        /// </remarks>
        public ConsumeResult<TKey, TValue> Consume(TimeSpan timeout)
            => (keyDeserializer != null && valueDeserializer != null)
                ? ConsumeImpl<TKey, TValue>(timeout.TotalMillisecondsAsInt(), keyDeserializer, valueDeserializer) // fast path for simple case
                : ConsumeViaBytes(timeout.TotalMillisecondsAsInt());
    }
}
