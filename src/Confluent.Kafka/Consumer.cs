// Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider
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
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Implements a high-level Apache Kafka consumer (with 
    ///     key and value deserialization).
    /// </summary>
    public class Consumer<TKey, TValue> : IDisposable
    {
        private readonly Consumer consumer;

        /// <summary>
        ///     The IDeserializer implementation instance used to deserialize keys.
        /// </summary>
        public IDeserializer<TKey> KeyDeserializer { get; }

        /// <summary>
        ///     The IDeserializer implementation instance used to deserialize values.
        /// </summary>
        public IDeserializer<TValue> ValueDeserializer { get; }

        /// <summary>
        ///     Creates a new Consumer instance.
        /// </summary>
        /// <param name="config">
        ///     librdkafka configuration parameters (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        /// </param>
        /// <param name="keyDeserializer">
        ///     An IDeserializer implementation instance for deserializing keys.
        /// </param>
        /// <param name="valueDeserializer">
        ///     An IDeserializer implementation instance for deserializing values.
        /// </param>
        public Consumer(
            IEnumerable<KeyValuePair<string, object>> config,
            IDeserializer<TKey> keyDeserializer,
            IDeserializer<TValue> valueDeserializer)
        {
            KeyDeserializer = keyDeserializer;
            ValueDeserializer = valueDeserializer;

            if (keyDeserializer != null && keyDeserializer == valueDeserializer)
            {
                throw new ArgumentException("Key and value deserializers must not be the same object.");
            }

            if (KeyDeserializer == null)
            {
                if (typeof(TKey) == typeof(Null))
                {
                    KeyDeserializer = (IDeserializer<TKey>)new NullDeserializer();
                }
                else if (typeof(TKey) == typeof(Ignore))
                {
                    KeyDeserializer = (IDeserializer<TKey>)new IgnoreDeserializer();
                }
                else
                {
                    throw new ArgumentNullException("Key deserializer must be specified.");
                }
            }

            if (ValueDeserializer == null)
            {
                if (typeof(TValue) == typeof(Null))
                {
                    ValueDeserializer = (IDeserializer<TValue>)new NullDeserializer();
                }
                else if (typeof(TValue) == typeof(Ignore))
                {
                    ValueDeserializer = (IDeserializer<TValue>)new IgnoreDeserializer();
                }
                else
                {
                    throw new ArgumentNullException("Value deserializer must be specified.");
                }
            }

            var configWithoutKeyDeserializerProperties = KeyDeserializer.Configure(config, true);
            var configWithoutValueDeserializerProperties = ValueDeserializer.Configure(config, false);

            var configWithoutDeserializerProperties = config.Where(item => 
                configWithoutKeyDeserializerProperties.Any(ci => ci.Key == item.Key) &&
                configWithoutValueDeserializerProperties.Any(ci => ci.Key == item.Key)
            );

            consumer = new Consumer(configWithoutDeserializerProperties);

            consumer.OnConsumeError += (sender, msg) 
                => OnConsumeError?.Invoke(this, msg);
        }

        /// <summary>
        ///     Poll for new messages / consumer events. Blocks until a new 
        ///     message or event is ready to be handled or the timeout period
        ///     <paramref name="millisecondsTimeout" /> has elapsed.
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
        public bool Consume(out Message<TKey, TValue> message, int millisecondsTimeout)
        {
            Message msg;
            if (!consumer.Consume(out msg, millisecondsTimeout))
            {
                message = null;
                return false;
            }

            try
            {
                message = msg.Deserialize(KeyDeserializer, ValueDeserializer);
            }
            catch (KafkaException ex)
            {
                var erroredMsg = new Message(
                    msg.Topic,
                    msg.Partition,
                    msg.Offset,
                    msg.Key,
                    msg.Value,
                    msg.Timestamp,
                    ex.Error
                );
                OnConsumeError?.Invoke(this, erroredMsg);
                message = null;
                return false;
            }

            return true;
        }

        /// <summary>
        ///     Refer to <see cref="Consume(out Message{TKey, TValue}, int)" />.
        /// </summary>
        public bool Consume(out Message<TKey, TValue> message, TimeSpan timeout)
            => Consume(out message, timeout.TotalMillisecondsAsInt());


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
        public void Poll(int millisecondsTimeout)
        {
            Message<TKey, TValue> msg;
            if (Consume(out msg, millisecondsTimeout))
            {
                OnMessage?.Invoke(this, msg);
            }
        }

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
        public void Poll(TimeSpan timeout)
        {
            Message<TKey, TValue> msg;
            if (Consume(out msg, timeout))
            {
                OnMessage?.Invoke(this, msg);
            }
        }

        /// <summary>
        ///     Poll for new consumer events, including new messages
        ///     ready to be consumed (which will trigger the OnMessage
        ///     event).
        /// </summary> 
        /// <remarks>
        ///     Blocks indefinitely until a new event is ready.
        /// </remarks>
        [Obsolete("Use an overload of Poll with a finite timeout.", false)]
        public void Poll()
            => Poll(-1);


        /// <summary>
        ///     Raised on new partition assignment.
        ///     You should typically call the Consumer.Assign method in this handler.
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<List<TopicPartition>> OnPartitionsAssigned
        {
            add { consumer.OnPartitionsAssigned += value; }
            remove { consumer.OnPartitionsAssigned -= value; }
        }

        /// <summary>
        ///     Raised when a partition assignment is revoked.
        ///     You should typically call the Consumer.Unassign method in this handler.
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<List<TopicPartition>> OnPartitionsRevoked
        {
            add { consumer.OnPartitionsRevoked += value; }
            remove { consumer.OnPartitionsRevoked -= value; }
        }

        /// <summary>
        ///     Raised to report the result of (automatic) offset commits.
        ///     Not raised as a result of the use of the CommitAsync method.
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<CommittedOffsets> OnOffsetsCommitted
        {
            add { consumer.OnOffsetsCommitted += value; }
            remove { consumer.OnOffsetsCommitted -= value; }
        }

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
        public event EventHandler<LogMessage> OnLog
        {
            add { consumer.OnLog += value; }
            remove { consumer.OnLog -= value; }
        }

        /// <summary>
        ///     Raised on librdkafka statistics events. JSON formatted
        ///     string as defined here: https://github.com/edenhill/librdkafka/wiki/Statistics
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<string> OnStatistics
        {
            add { consumer.OnStatistics += value; }
            remove { consumer.OnStatistics -= value; }
        }

        /// <summary>
        ///     Raised when a consumed message has an error != NoError (both when Consume or Poll is used for polling).
        ///     Also raised on deserialization errors.
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<Message> OnConsumeError;

        /// <summary>
        ///     Raised on critical errors, e.g. connection failures or all 
        ///     brokers down. Note that the client will try to automatically 
        ///     recover from errors - these errors should be seen as 
        ///     informational rather than catastrophic
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<Error> OnError
        {
            add { consumer.OnError += value; }
            remove { consumer.OnError -= value; }
        }

        /// <summary>
        ///     Raised when the consumer reaches the end of a topic/partition it is reading from.
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<TopicPartitionOffset> OnPartitionEOF
        {
            add { consumer.OnPartitionEOF += value; }
            remove { consumer.OnPartitionEOF -= value; }
        }

        /// <summary>
        ///     Raised when a new message is avaiable for consumption. NOT raised when Consumer.Consume
        ///     is used for polling (only when Consmer.Poll is used for polling). NOT raised when the 
        ///     message has an Error (OnConsumeError is raised in that case).
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<Message<TKey, TValue>> OnMessage;

        /// <summary>
        ///     Gets the current partition assignment as set by Assign.
        /// </summary>
        public List<TopicPartition> Assignment
            => consumer.Assignment;

        /// <summary>
        ///     Gets the current partition subscription as set by Subscribe.
        /// </summary>
        public List<string> Subscription
            => consumer.Subscription;

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
        public void Subscribe(IEnumerable<string> topics)
            => consumer.Subscribe(topics);

        /// <summary>
        ///     Update the subscription set to a single topic.
        ///
        ///     Any previous subscription will be unassigned and unsubscribed first.
        /// </summary>
        public void Subscribe(string topic)
            => consumer.Subscribe(topic);

        /// <summary>
        ///     Unsubscribe from the current subscription set.
        /// </summary>
        public void Unsubscribe()
            => consumer.Unsubscribe();

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
        public void Assign(IEnumerable<TopicPartitionOffset> partitions)
            => consumer.Assign(partitions);

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
        public void Assign(IEnumerable<TopicPartition> partitions)
            => consumer.Assign(partitions);

        /// <summary>
        ///     Stop consumption and remove the current assignment.
        /// </summary>
        public void Unassign()
            => consumer.Unassign();

        /// <summary>
        ///     Store offsets for a single partition based on the topic/partition/offset
        ///     of a message.
        ///     
        ///     The offset will be committed (written) to the offset store according
        ///     to `auto.commit.interval.ms` or manual offset-less commit().
        /// </summary>
        /// <remarks>
        ///     `enable.auto.offset.store` must be set to "false" when using this API.
        /// </remarks>
        /// <param name="message">
        ///     A message used to determine the offset to store and topic/partition.
        /// </param>
        /// <returns>
        ///     Current stored offset or a partition specific error.
        /// </returns>
        public TopicPartitionOffsetError StoreOffset(Message<TKey, TValue> message)
            => consumer.StoreOffsets(new[] { new TopicPartitionOffset(message.TopicPartition, message.Offset + 1) })[0];

        /// <include file='include_docs.xml' path='API/Member[@name="Store_Offsets"]/*' />
        public List<TopicPartitionOffsetError> StoreOffsets(IEnumerable<TopicPartitionOffset> offsets)
            => consumer.StoreOffsets(offsets);

        /// <summary>
        ///     Commit offsets for the current assignment.
        /// </summary>
        public Task<CommittedOffsets> CommitAsync()
            => consumer.CommitAsync();

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
        public Task<CommittedOffsets> CommitAsync(Message<TKey, TValue> message)
            => consumer.CommitAsync(new[] { new TopicPartitionOffset(message.TopicPartition, message.Offset + 1) });

        /// <summary>
        ///     Commit an explicit list of offsets.
        /// </summary>
        public Task<CommittedOffsets> CommitAsync(IEnumerable<TopicPartitionOffset> offsets)
            => consumer.CommitAsync(offsets);

        /// <summary>
        ///     Releases all resources used by this Consumer.
        /// 
        ///     This call will block until the consumer has revoked its assignment, 
        ///     calling the rebalance event if it is configured, committed offsets to 
        ///     broker, and left the consumer group.
        /// 
        ///     [UNSTABLE-API] - The Dispose method should not block. We will
        ///     separate out consumer close functionality from this method.
        /// </summary>
        public void Dispose()
            => consumer.Dispose();

        /// <include file='include_docs.xml' path='API/Member[@name="Consumer_Seek"]/*' />
        public void Seek(TopicPartitionOffset tpo)
            => consumer.Seek(tpo);

        /// <include file='include_docs.xml' path='API/Member[@name="Consumer_Pause"]/*' />
        public List<TopicPartitionError> Pause(IEnumerable<TopicPartition> partitions)
            => consumer.Pause(partitions);

        /// <include file='include_docs.xml' path='API/Member[@name="Consumer_Resume"]/*' />
        public List<TopicPartitionError> Resume(IEnumerable<TopicPartition> partitions)
            => consumer.Resume(partitions);

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
        public List<TopicPartitionOffsetError> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
            => consumer.Committed(partitions, timeout);

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
        public List<TopicPartitionOffsetError> Position(IEnumerable<TopicPartition> partitions)
            => consumer.Position(partitions);

        /// <summary>
        ///     Gets the name of this consumer instance.
        ///     Contains (but is not equal to) the client.id configuration parameter.
        /// </summary>
        /// <remarks>
        ///     This name will be unique across all consumer instances
        ///     in a given application which allows log messages to be
        ///     associated with the corresponding instance.
        /// </remarks>
        public string Name
            => consumer.Name;

        /// <summary>
        ///     Gets the (dynamic) group member id of this consumer (as set by
        ///     the broker).
        /// </summary>
        public string MemberId
            => consumer.MemberId;


        /// <summary>
        ///     Get information pertaining to all groups in the Kafka cluster (blocking).
        ///
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
        /// </param>
        public List<GroupInfo> ListGroups(TimeSpan timeout)
            => consumer.ListGroups(timeout);


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
        public GroupInfo ListGroup(string group, TimeSpan timeout)
            => consumer.ListGroup(group, timeout);

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
        public GroupInfo ListGroup(string group)
            => consumer.ListGroup(group);


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
        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
            => consumer.GetWatermarkOffsets(topicPartition);


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
        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
            => consumer.QueryWatermarkOffsets(topicPartition, timeout);

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
        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition)
            => consumer.QueryWatermarkOffsets(topicPartition);

        /// <include file='include_docs.xml' path='API/Member[@name="Consumer_OffsetsForTimes"]/*' />
        public IEnumerable<TopicPartitionOffsetError> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout)
            => consumer.OffsetsForTimes(timestampsToSearch, timeout);

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Producer.GetMetadata(bool,string,int)" /> for more information.
        ///     
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        public Metadata GetMetadata(bool allTopics, TimeSpan timeout)
            => consumer.GetMetadata(allTopics, timeout);

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Producer.GetMetadata(bool,string,int)" /> for more information.
        ///     
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        public Metadata GetMetadata(bool allTopics)
            => consumer.GetMetadata(allTopics);

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
        public int AddBrokers(string brokers)
            => consumer.AddBrokers(brokers);
    }

    /// <summary>
    ///     Implements a high-level Apache Kafka consumer (without deserialization).
    /// 
    ///     [UNSTABLE-API] We are considering making this class private in a future version 
    ///     so as to limit API surface area. Prefer to use the deserializing consumer
    ///     <see cref="Confluent.Kafka.Consumer{TKey,TValue}" /> where possible.
    /// </summary>
    public class Consumer : IDisposable
    {
        private SafeKafkaHandle kafkaHandle;

        private LibRdKafka.ErrorDelegate errorDelegate;
        private void ErrorCallback(IntPtr rk, ErrorCode err, string reason, IntPtr opaque)
        {
            OnError?.Invoke(this, new Error(err, reason));
        }

        private LibRdKafka.StatsDelegate statsDelegate;
        private int StatsCallback(IntPtr rk, IntPtr json, UIntPtr json_len, IntPtr opaque)
        {
            OnStatistics?.Invoke(this, Util.Marshal.PtrToStringUTF8(json));
            return 0; // instruct librdkafka to immediately free the json ptr.
        }

        private LibRdKafka.LogDelegate logDelegate;
        private void LogCallback(IntPtr rk, int level, string fac, string buf)
        {
            var name = Util.Marshal.PtrToStringUTF8(LibRdKafka.name(rk));

            if (OnLog == null)
            {
                // A stderr logger is used by default if none is specified.
                Loggers.ConsoleLogger(this, new LogMessage(name, level, fac, buf));
                return;
            }

            OnLog?.Invoke(this, new LogMessage(name, level, fac, buf));
        }

        private LibRdKafka.RebalanceDelegate rebalanceDelegate;
        private void RebalanceCallback(
            IntPtr rk,
            ErrorCode err,
            /* rd_kafka_topic_partition_list_t * */ IntPtr partitions,
            IntPtr opaque)
        {
            var partitionList = SafeKafkaHandle.GetTopicPartitionOffsetErrorList(partitions).Select(p => p.TopicPartition).ToList();
            if (err == ErrorCode.Local_AssignPartitions)
            {
                var handler = OnPartitionsAssigned;
                if (handler != null && handler.GetInvocationList().Length > 0)
                {
                    handler(this, partitionList);
                }
                else
                {
                    Assign(partitionList.Select(p => new TopicPartitionOffset(p, Offset.Invalid)));
                }
            }
            if (err == ErrorCode.Local_RevokePartitions)
            {
                var handler = OnPartitionsRevoked;
                if (handler != null && handler.GetInvocationList().Length > 0)
                {
                    handler(this, partitionList);
                }
                else
                {
                    Unassign();
                }
            }
        }

        private LibRdKafka.CommitDelegate commitDelegate;
        private void CommitCallback(
            IntPtr rk,
            ErrorCode err,
            /* rd_kafka_topic_partition_list_t * */ IntPtr offsets,
            IntPtr opaque)
        {
            OnOffsetsCommitted?.Invoke(this, new CommittedOffsets(
                SafeKafkaHandle.GetTopicPartitionOffsetErrorList(offsets),
                new Error(err)
            ));
        }

        /// <summary>
        ///     Create a new consumer with the supplied configuration.
        /// </summary>
        /// <remarks>
        ///     Refer to: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        /// </remarks>
        public Consumer(IEnumerable<KeyValuePair<string, object>> config)
        {
            LibRdKafka.Initialize(null);

            if (config.FirstOrDefault(prop => string.Equals(prop.Key, "group.id", StringComparison.Ordinal)).Value == null)
            {
                throw new ArgumentException("'group.id' configuration parameter is required and was not specified.");
            }

            var configHandle = SafeConfigHandle.Create();
            config
                .Where(prop => prop.Key != "default.topic.config")
                .ToList()
                .ForEach((kvp) => { configHandle.Set(kvp.Key, kvp.Value.ToString()); });

            // Note: Setting default topic configuration properties via default.topic.config is depreciated 
            // and this functionality will be removed in a future version of the library.
            var defaultTopicConfig = (IEnumerable<KeyValuePair<string, object>>)config.FirstOrDefault(prop => prop.Key == "default.topic.config").Value;
            if (defaultTopicConfig != null)
            {
                defaultTopicConfig.ToList().ForEach(
                    (kvp) => { configHandle.Set(kvp.Key, kvp.Value.ToString()); }
                );
            }

            // Explicitly keep references to delegates so they are not reclaimed by the GC.
            rebalanceDelegate = RebalanceCallback;
            commitDelegate = CommitCallback;
            errorDelegate = ErrorCallback;
            logDelegate = LogCallback;
            statsDelegate = StatsCallback;

            IntPtr configPtr = configHandle.DangerousGetHandle();

            LibRdKafka.conf_set_rebalance_cb(configPtr, rebalanceDelegate);
            LibRdKafka.conf_set_offset_commit_cb(configPtr, commitDelegate);

            LibRdKafka.conf_set_error_cb(configPtr, errorDelegate);
            LibRdKafka.conf_set_log_cb(configPtr, logDelegate);
            LibRdKafka.conf_set_stats_cb(configPtr, statsDelegate);

            this.kafkaHandle = SafeKafkaHandle.Create(RdKafkaType.Consumer, configPtr);
            configHandle.SetHandleAsInvalid(); // config object is no longer useable.

            var pollSetConsumerError = kafkaHandle.PollSetConsumer();
            if (pollSetConsumerError != ErrorCode.NoError)
            {
                throw new KafkaException(new Error(pollSetConsumerError,
                    $"Failed to redirect the poll queue to consumer_poll queue: {ErrorCodeExtensions.GetReason(pollSetConsumerError)}"));
            }
        }

        /// <summary>
        ///     Raised on new partition assignment.
        ///     You should typically call the Consumer.Assign method in this handler.
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<List<TopicPartition>> OnPartitionsAssigned;

        /// <summary>
        ///     Raised when a partition assignment is revoked.
        ///     You should typically call the Consumer.Unassign method in this handler.
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<List<TopicPartition>> OnPartitionsRevoked;

        /// <summary>
        ///     Raised to report the result of (automatic) offset commits.
        ///     Not raised as a result of the use of the CommitAsync method.
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<CommittedOffsets> OnOffsetsCommitted;

        /// <summary>
        ///     Raised on critical errors, e.g. connection failures or all 
        ///     brokers down. Note that the client will try to automatically 
        ///     recover from errors - these errors should be seen as 
        ///     informational rather than catastrophic
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<Error> OnError;

        /// <summary>
        ///     Raised when a consumed message has an error != NoError (both when Consume or Poll is used for polling).
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<Message> OnConsumeError;

        /// <summary>
        ///     Raised on librdkafka statistics events. JSON formatted
        ///     string as defined here: https://github.com/edenhill/librdkafka/wiki/Statistics
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<string> OnStatistics;

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
        public event EventHandler<LogMessage> OnLog;

        /// <summary>
        ///     Raised when a new message is avaiable for consumption. NOT raised when Consumer.Consume
        ///     is used for polling (only when Consmer.Poll is used for polling). NOT raised when the 
        ///     message has an Error (OnConsumeError is raised in that case).
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<Message> OnMessage;

        /// <summary>
        ///     Raised when the consumer reaches the end of a topic/partition it is reading from.
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<TopicPartitionOffset> OnPartitionEOF;


        /// <summary>
        ///     Gets the current partition assignment as set by Assign.
        /// </summary>
        public List<TopicPartition> Assignment
            => kafkaHandle.GetAssignment();

        /// <summary>
        ///     Gets the current topic subscription as set by Subscribe.
        /// </summary>
        public List<string> Subscription
            => kafkaHandle.GetSubscription();

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
        public void Subscribe(IEnumerable<string> topics)
            => kafkaHandle.Subscribe(topics);

        /// <summary>
        ///     Update the subscription set to a single topic.
        ///
        ///     Any previous subscription will be unassigned and unsubscribed first.
        /// </summary>
        public void Subscribe(string topic)
            => Subscribe(new[] { topic });

        /// <summary>
        ///     Unsubscribe from the current subscription set.
        /// </summary>
        public void Unsubscribe()
            => kafkaHandle.Unsubscribe();

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
        public void Assign(TopicPartition partition)
            => this.Assign(new List<TopicPartition> { partition });

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
        public void Assign(TopicPartitionOffset partition)
            => this.Assign(new List<TopicPartitionOffset> { partition });

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
        ///     according to the 'auto.offset.reset' configuration parameter
        ///     if no offsets have been committed yet.
        /// </param>
        public void Assign(IEnumerable<TopicPartitionOffset> partitions)
            => kafkaHandle.Assign(partitions.ToList());

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
        public void Assign(IEnumerable<TopicPartition> partitions)
            => kafkaHandle.Assign(partitions.Select(p => new TopicPartitionOffset(p, Offset.Invalid)).ToList());

        /// <summary>
        ///     Stop consumption and remove the current topic/partition assignment.
        /// </summary>
        public void Unassign()
            => kafkaHandle.Assign(null);

        /// <summary>
        ///     Poll for new messages / consumer events. Blocks until a new 
        ///     message or event is ready to be handled or the timeout period
        ///     <paramref name="millisecondsTimeout" /> has elapsed.
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
        public bool Consume(out Message message, int millisecondsTimeout)
        {
            if (kafkaHandle.ConsumerPoll(out message, (IntPtr)millisecondsTimeout))
            {
                switch (message.Error.Code)
                {
                    case ErrorCode.NoError:
                        return true;
                    case ErrorCode.Local_PartitionEOF:
                        OnPartitionEOF?.Invoke(this, message.TopicPartitionOffset);
                        return false;
                    default:
                        OnConsumeError?.Invoke(this, message);
                        return false;
                }
            }

            return false;
        }

        /// <summary>
        ///     Refer to <see cref="Consume(out Message, int)" />
        /// </summary>
        public bool Consume(out Message message, TimeSpan timeout)
            => Consume(out message, timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     Poll for new consumer events, including new messages
        ///     ready to be consumed (which will trigger the OnMessage
        ///     event).
        /// </summary>
        /// <param name="timeout"> 
        ///     The maximum time to block. You should typically use a
        ///     relatively short timout period because this operation
        ///     cannot be cancelled.
        /// </param>
        public void Poll(TimeSpan timeout)
        {
            Message msg;
            if (Consume(out msg, timeout))
            {
                OnMessage?.Invoke(this, msg);
            }
        }

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
        public void Poll(int millisecondsTimeout)
        {
            Message msg;
            if (Consume(out msg, millisecondsTimeout))
            {
                OnMessage?.Invoke(this, msg);
            }
        }

        /// <summary>
        ///     Poll for new consumer events, including new messages
        ///     ready to be consumed (which will trigger the OnMessage
        ///     event).
        /// </summary> 
        /// <remarks>
        ///     Blocks indefinitely until a new event is ready.
        /// </remarks>
        [Obsolete("Use an overload of Poll with a finite timeout.", false)]
        public void Poll()
            => Poll(-1);


        /// <include file='include_docs.xml' path='API/Member[@name="Store_Offsets"]/*' />
        public List<TopicPartitionOffsetError> StoreOffsets(IEnumerable<TopicPartitionOffset> offsets)
            => kafkaHandle.StoreOffsets(offsets);


        /// <summary>
        ///     Commit offsets for the current assignment.
        /// </summary>
        public Task<CommittedOffsets> CommitAsync()
            => kafkaHandle.CommitAsync();

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
        public Task<CommittedOffsets> CommitAsync(Message message)
        {
            if (message.Error.Code != ErrorCode.NoError)
            {
                throw new InvalidOperationException("Must not commit offset for errored message");
            }
            return CommitAsync(new[] { new TopicPartitionOffset(message.TopicPartition, message.Offset + 1) });
        }

        /// <summary>
        ///     Commit an explicit list of offsets.
        /// </summary>
        public Task<CommittedOffsets> CommitAsync(IEnumerable<TopicPartitionOffset> offsets)
            => kafkaHandle.CommitAsync(offsets);

        /// <include file='include_docs.xml' path='API/Member[@name="Consumer_Seek"]/*' />
        public void Seek(TopicPartitionOffset tpo)
            => kafkaHandle.Seek(tpo.Topic, tpo.Partition, tpo.Offset, -1);

        /// <include file='include_docs.xml' path='API/Member[@name="Consumer_Pause"]/*' />
        public List<TopicPartitionError> Pause(IEnumerable<TopicPartition> partitions)
            => kafkaHandle.Pause(partitions);

        /// <include file='include_docs.xml' path='API/Member[@name="Consumer_Resume"]/*' />
        public List<TopicPartitionError> Resume(IEnumerable<TopicPartition> partitions)
            => kafkaHandle.Resume(partitions);

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
        public List<TopicPartitionOffsetError> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
            => kafkaHandle.Committed(partitions, (IntPtr) timeout.TotalMillisecondsAsInt());

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
        public List<TopicPartitionOffsetError> Position(IEnumerable<TopicPartition> partitions)
            => kafkaHandle.Position(partitions);

        /// <summary>
        ///     Releases all resources used by this Consumer.
        /// 
        ///     This call will block until the consumer has revoked its assignment, 
        ///     calling the rebalance event if it is configured, committed offsets to 
        ///     broker, and left the consumer group.
        /// 
        ///     [UNSTABLE-API] - The Dispose method should not block. We will
        ///     separate out consumer close functionality from this method.
        /// </summary>
        public void Dispose()
        {
            kafkaHandle.ConsumerClose();
            kafkaHandle.Dispose();
        }

        /// <summary>
        ///     Gets the name of this consumer instance.
        ///     Contains (but is not equal to) the client.id configuration parameter.
        /// </summary>
        /// <remarks>
        ///     This name will be unique across all consumer instances
        ///     in a given application which allows log messages to be
        ///     associated with the corresponding instance.
        /// </remarks>
        public string Name
            => kafkaHandle.Name;

        /// <summary>
        ///     Gets the (dynamic) group member id of this consumer (as set by
        ///     the broker).
        /// </summary>
        public string MemberId
            => kafkaHandle.MemberId;


        /// <summary>
        ///     Get information pertaining to all groups in the Kafka cluster (blocking).
        ///
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
        /// </param>
        public List<GroupInfo> ListGroups(TimeSpan timeout)
            => kafkaHandle.ListGroups(timeout.TotalMillisecondsAsInt());


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
        public GroupInfo ListGroup(string group, TimeSpan timeout)
            => kafkaHandle.ListGroup(group, timeout.TotalMillisecondsAsInt());

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
        public GroupInfo ListGroup(string group)
            => kafkaHandle.ListGroup(group, -1);

        /// <summary>
        ///     Get last known low (oldest/beginning) and high (newest/end)
        ///     offsets for a topic/partition.
        ///     
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        /// <remarks>
        ///     The low offset is updated periodically (if statistics.interval.ms is set)
        ///     while the high offset is updated on each fetched message set from the broker.
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
        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
            => kafkaHandle.GetWatermarkOffsets(topicPartition.Topic, topicPartition.Partition);

        /// <include file='include_docs.xml' path='API/Member[@name="Consumer_OffsetsForTimes"]/*' />
        public IEnumerable<TopicPartitionOffsetError> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout)
            => kafkaHandle.OffsetsForTimes(timestampsToSearch, timeout.TotalMillisecondsAsInt());

        /// <summary>
        ///     Query the Kafka cluster for low (oldest/beginning) and high (newest/end)
        ///     offsets for the specified topic/partition (blocking)
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
        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
            => kafkaHandle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, timeout.TotalMillisecondsAsInt());

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
        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition)
            => kafkaHandle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, -1);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Producer.GetMetadata(bool,string,int)" /> for more information.
        ///     
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        public Metadata GetMetadata(bool allTopics, TimeSpan timeout)
            => kafkaHandle.GetMetadata(allTopics, null, timeout.TotalMillisecondsAsInt());

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Producer.GetMetadata(bool,string,int)" /> for more information.
        ///     
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        public Metadata GetMetadata(bool allTopics)
            => kafkaHandle.GetMetadata(allTopics, null, -1);

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
        public int AddBrokers(string brokers)
            => kafkaHandle.AddBrokers(brokers);
    }
}
