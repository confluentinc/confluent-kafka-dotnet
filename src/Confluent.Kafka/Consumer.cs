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
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka
{
    public class Consumer<TKey, TValue> : IDisposable
    {
        protected readonly Consumer consumer;

        public IDeserializer<TKey> KeyDeserializer { get; }

        public IDeserializer<TValue> ValueDeserializer { get; }

        public Consumer(
            IEnumerable<KeyValuePair<string, object>> config,
            IDeserializer<TKey> keyDeserializer,
            IDeserializer<TValue> valueDeserializer)
        {
            KeyDeserializer = keyDeserializer;
            ValueDeserializer = valueDeserializer;

            // TODO: allow deserializers to be set in the producer config IEnumerable<KeyValuePair<string, object>>.

            if (KeyDeserializer == null)
            {
                if (typeof(TKey) != typeof(Null))
                {
                    throw new ArgumentNullException("Key deserializer must be specified.");
                }
                // TKey == Null -> cast is always valid.
                KeyDeserializer = (IDeserializer<TKey>)new NullDeserializer();
            }

            if (ValueDeserializer == null)
            {
                if (typeof(TValue) != typeof(Null))
                {
                    throw new ArgumentNullException("Value deserializer must be specified.");
                }
                // TValue == Null -> cast is always valid.
                ValueDeserializer = (IDeserializer<TValue>)new NullDeserializer();
            }

            consumer = new Consumer(config);
            consumer.OnLog += (sender, e) => OnLog?.Invoke(sender, e);
            consumer.OnError += (sender, e) => OnError?.Invoke(sender, e);
            consumer.OnStatistics += (sender, e) => OnStatistics?.Invoke(sender, e);
            consumer.OnPartitionsAssigned += (sender, e) => OnPartitionsAssigned?.Invoke(sender, e);
            consumer.OnPartitionsRevoked += (sender, e) => OnPartitionsRevoked?.Invoke(sender, e);
            consumer.OnOffsetCommit += (sender, e) => OnOffsetCommit?.Invoke(sender, e);
            // TODO: bypass this.consumer for this event to optimize perf.
            consumer.OnMessage += (sender, e) => OnMessage?.Invoke(sender,
                new Message<TKey, TValue> (
                    e.Topic,
                    e.Partition,
                    e.Offset,
                    KeyDeserializer.Deserialize(e.Key),
                    ValueDeserializer.Deserialize(e.Value),
                    e.Timestamp,
                    e.Error
                )
            );
            consumer.OnPartitionEOF += (sender, e) => OnPartitionEOF?.Invoke(sender, e);
        }


        public bool Consume(out Message<TKey, TValue> message, int millisecondsTimeout)
        {
            Message msg;
            if (!consumer.Consume(out msg, millisecondsTimeout))
            {
                message = default(Message<TKey, TValue>);
                return false;
            }

            message = msg.Deserialize(KeyDeserializer, ValueDeserializer);
            return true;
        }

        public bool Consume(out Message<TKey, TValue> message, TimeSpan timeout)
            => Consume(out message, timeout.TotalMillisecondsAsInt());

        public bool Consume(out Message<TKey, TValue> message)
            => Consume(out message, -1);


        public void Poll(int millisecondsTimeout)
        {
            Message<TKey, TValue> msg;
            if (Consume(out msg, millisecondsTimeout))
            {
                OnMessage?.Invoke(this, msg);
            }
        }

        public void Poll(TimeSpan timeout)
        {
            Message<TKey, TValue> msg;
            if (Consume(out msg, timeout))
            {
                OnMessage?.Invoke(this, msg);
            }
        }

        public void Poll()
            => Poll(-1);

        public void Start()
            => consumer.StartBackgroundPollLoop((Action<int>)Poll);

        public void Stop()
            => consumer.Stop();

        public event EventHandler<List<TopicPartition>> OnPartitionsAssigned;

        public event EventHandler<List<TopicPartition>> OnPartitionsRevoked;

        public event EventHandler<CommittedOffsets> OnOffsetCommit;

        public event EventHandler<LogMessage> OnLog;

        public event EventHandler<string> OnStatistics;

        public event EventHandler<Error> OnError;

        public event EventHandler<Message<TKey, TValue>> OnMessage;

        public event EventHandler<TopicPartitionOffset> OnPartitionEOF;


        /// <summary>
        ///     Returns the current partition assignment as set by Assign.
        /// </summary>
        public List<TopicPartition> Assignment
            => consumer.Assignment;

        /// <summary>
        ///     Returns the current partition subscription as set by Subscribe.
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
        public void Subscribe(ICollection<string> topics)
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
        ///     Update the assignment set to \p partitions.
        ///
        ///     The assignment set is the set of partitions actually being consumed
        ///     by the KafkaConsumer.
        /// </summary>
        /// <remarks>
        ///     blocks
        /// </remarks>
        public void Assign(IEnumerable<TopicPartitionOffset> partitions)
            => consumer.Assign(partitions);

        /// <summary>
        ///     Stop consumption and remove the current assignment.
        /// </summary>
        public void Unassign()
            => consumer.Unassign();

        /// <summary>
        ///     Commit offsets for the current assignment.
        /// </summary>
        public void Commit()
            => consumer.Commit();

        /// <summary>
        ///     Commits an offset based on the topic/partition/offset of a message.
        ///     The next message to be read will be that following <param name="message" />.
        /// </summary>
        /// <remarks>
        ///     A consumer which has position N has consumed records with offsets 0 through N-1 and will next receive the record with offset N.
        ///     Hence, this method commits an offset of <param name="message">.Offset + 1.
        /// </remarks>
        public void Commit(Message<TKey, TValue> message)
            => consumer.Commit(new List<TopicPartitionOffset> { new TopicPartitionOffset(message.TopicPartition, message.Offset + 1) });

        /// <summary>
        ///     Commit explicit list of offsets.
        /// </summary>
        public void Commit(ICollection<TopicPartitionOffset> offsets)
            => consumer.Commit(offsets);

        public void Dispose()
            => consumer.Dispose();

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
        public List<TopicPartitionOffsetError> Committed(ICollection<TopicPartition> partitions, TimeSpan timeout)
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
        public List<TopicPartitionOffsetError> Position(ICollection<TopicPartition> partitions)
            => consumer.Position(partitions);

        public string Name
            => consumer.Name;

        public void Flush()
            => consumer.Flush();

        public string MemberId
            => consumer.MemberId;

        /// <summary>
        ///     The current librdkafka out queue length.
        /// </summary>
        /// <remarks>
        ///     The out queue contains messages and requests waiting to be sent to,
        ///     or acknowledged by, the broker.
        /// </summary>
        public long OutQueueLength
            => consumer.OutQueueLength;


        public List<GroupInfo> ListGroups(TimeSpan timeout)
            => consumer.ListGroups(timeout);

        public List<GroupInfo> ListGroups()
            => consumer.ListGroups();


        public GroupInfo ListGroup(string group, TimeSpan timeout)
            => consumer.ListGroup(group, timeout);

        public GroupInfo ListGroup(string group)
            => consumer.ListGroup(group);


        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
            => consumer.GetWatermarkOffsets(topicPartition);


        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
            => consumer.QueryWatermarkOffsets(topicPartition, timeout);

        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition)
            => consumer.QueryWatermarkOffsets(topicPartition);


        public Metadata GetMetadata(bool allTopics, TimeSpan timeout)
            => consumer.GetMetadata(allTopics, timeout);

        public Metadata GetMetadata(bool allTopics)
            => consumer.GetMetadata(allTopics);
    }

    public class Consumer : IDisposable
    {
        private CancellationTokenSource consumerCts = null;
        private Task consumerTask = null;

        private SafeKafkaHandle kafkaHandle;

        private void ErrorCallback(IntPtr rk, ErrorCode err, string reason, IntPtr opaque)
        {
            // TODO: Is reason ever different from that returned by err2str?
            //       If so, sort something else out here.
            OnError?.Invoke(this, err);
        }

        private int StatsCallback(IntPtr rk, IntPtr json, UIntPtr json_len, IntPtr opaque)
        {
            OnStatistics?.Invoke(this, Util.Marshal.PtrToStringUTF8(json));
            return 0; // instruct librdkafka to immediately free the json ptr.
        }

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

        // Explicitly keep reference to delegate so it stays alive
        private LibRdKafka.RebalanceDelegate rebalanceDelegate;
        private void RebalanceCallback(
            IntPtr rk,
            ErrorCode err,
            /* rd_kafka_topic_partition_list_t * */ IntPtr partitions,
            IntPtr opaque)
        {
            var partitionList = SafeKafkaHandle.GetTopicPartitionOffsetErrorList(partitions).Select(p => p.TopicPartition).ToList();
            if (err == ErrorCode._ASSIGN_PARTITIONS)
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
            if (err == ErrorCode._REVOKE_PARTITIONS)
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

        // Explicitly keep reference to delegate so it stays alive
        private LibRdKafka.CommitDelegate commitDelegate;
        private void CommitCallback(
            IntPtr rk,
            ErrorCode err,
            /* rd_kafka_topic_partition_list_t * */ IntPtr offsets,
            IntPtr opaque)
        {
            OnOffsetCommit?.Invoke(this, new CommittedOffsets(
                // TODO: check to see whether errors can ever be present here. If so, expose TPOE, not TPO.
                SafeKafkaHandle.GetTopicPartitionOffsetErrorList(offsets).Select(tp => tp.TopicPartitionOffset).ToList(),
                err
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
            var defaultTopicConfig = (IEnumerable<KeyValuePair<string, object>>)config.FirstOrDefault(prop => prop.Key == "default.topic.config").Value;
            var configHandle = SafeConfigHandle.Create();
            config
                .Where(prop => prop.Key != "default.topic.config")
                .ToList()
                .ForEach((kvp) => { configHandle.Set(kvp.Key, kvp.Value.ToString()); });

            rebalanceDelegate = RebalanceCallback;
            commitDelegate = CommitCallback;

            IntPtr configPtr = configHandle.DangerousGetHandle();

            LibRdKafka.conf_set_rebalance_cb(configPtr, rebalanceDelegate);
            LibRdKafka.conf_set_offset_commit_cb(configPtr, commitDelegate);
            if (defaultTopicConfig != null)
            {
                var topicConfigHandle = SafeTopicConfigHandle.Create();
                if (config != null)
                {
                    defaultTopicConfig.ToList().ForEach((kvp) => { topicConfigHandle.Set(kvp.Key, kvp.Value.ToString()); });
                }
                LibRdKafka.conf_set_default_topic_conf(configPtr, topicConfigHandle.DangerousGetHandle());
            }

            LibRdKafka.conf_set_error_cb(configPtr, ErrorCallback);
            LibRdKafka.conf_set_log_cb(configPtr, LogCallback);
            LibRdKafka.conf_set_stats_cb(configPtr, StatsCallback);

            this.kafkaHandle = SafeKafkaHandle.Create(RdKafkaType.Consumer, configPtr);

            var pollSetConsumerError = kafkaHandle.PollSetConsumer();
            if (pollSetConsumerError != ErrorCode.NO_ERROR)
            {
                throw new KafkaException(pollSetConsumerError, "Failed to redirect the poll queue to consumer_poll queue");
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
        ///     TODO: can a partial revokation ever happen? What happens in this case?
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<List<TopicPartition>> OnPartitionsRevoked;

        /// <summary>
        ///     An event used to report on the result of offset commits - all commits, whether manual or automatic.
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<CommittedOffsets> OnOffsetCommit;

        /// <summary>
        ///     Raised on critical errors, e.g. connection failures or all brokers down.
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<Error> OnError;

        /// <summary>
        ///     Raised on librdkafka statistics events. These can be enabled by setting the statistics.interval.ms configuration parameter.
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<string> OnStatistics;

        /// <summary>
        ///     Raised when there is information that should be logged.
        ///     You can set the log severity level using the "log_level" configuration parameter
        /// </summary>
        /// <remarks>
        ///     Executes on potentially any internal librdkafka thread.
        ///     Do not call any librdkafka methods from within handlers of this event.
        /// </remarks>
        public event EventHandler<LogMessage> OnLog;

        /// <summary>
        ///     Raised when a new message is avaiable for consumption. NOT raised when Consumer.Consume
        ///     is used for polling.
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
        ///     Returns the current partition assignment as set by Assign.
        /// </summary>
        public List<TopicPartition> Assignment
            => kafkaHandle.GetAssignment();

        /// <summary>
        ///     Returns the current topic subscription as set by Subscribe.
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
        public void Subscribe(ICollection<string> topics)
            => kafkaHandle.Subscribe(topics);

        /// <summary>
        ///     Update the subscription set to a single topic.
        ///
        ///     Any previous subscription will be unassigned and unsubscribed first.
        /// </summary>
        public void Subscribe(string topic)
            => Subscribe(new List<string> { topic });

        /// <summary>
        ///     Unsubscribe from the current subscription set.
        /// </summary>
        public void Unsubscribe()
            => kafkaHandle.Unsubscribe();

        /// <summary>
        ///     Update the assignment set to \p partitions.
        ///
        ///     The assignment set is the set of partitions actually being consumed
        ///     by the KafkaConsumer.
        /// </summary>
        /// <remarks>
        ///     blocks
        /// </remarks>
        public void Assign(IEnumerable<TopicPartitionOffset> partitions)
            => kafkaHandle.Assign(partitions.ToList());

        /// <summary>
        ///     Stop consumption and remove the current assignment.
        /// </summary>
        public void Unassign()
            => kafkaHandle.Assign(null);

        /// <summary>
        ///     Manually consume message or triggers events.
        ///
        ///     Will invoke events for OnPartitionsAssigned/Revoked,
        ///     OnOffsetCommit, etc. on the calling thread.
        /// </summary>
        public bool Consume(out Message message, int millisecondsTimeout)
        {
            if (kafkaHandle.ConsumerPoll(out message, (IntPtr)millisecondsTimeout))
            {
                switch (message.Error.Code)
                {
                    case ErrorCode.NO_ERROR:
                        return true;
                    case ErrorCode._PARTITION_EOF:
                        OnPartitionEOF?.Invoke(this, message.TopicPartitionOffset);
                        return false;
                    default:
                        OnError?.Invoke(this, message.Error);
                        return false;
                }
            }

            return false;
        }

        public bool Consume(out Message message, TimeSpan timeout)
            => Consume(out message, timeout.TotalMillisecondsAsInt());

        public bool Consume(out Message message)
            => Consume(out message, -1);

        public void Poll(TimeSpan timeout)
        {
            if (consumerCts != null)
            {
                throw new Exception("Cannot call Poll on Consumer with background poll thread running.");
            }

            Message msg;
            if (Consume(out msg, timeout))
            {
                OnMessage?.Invoke(this, msg);
            }
        }

        public void Poll(int millisecondsTimeout)
        {
            if (consumerCts != null)
            {
                throw new Exception("Cannot call Poll on Consumer with background poll thread running.");
            }

            Message msg;
            if (Consume(out msg, millisecondsTimeout))
            {
                OnMessage?.Invoke(this, msg);
            }
        }

        public void Poll()
            => Poll(-1);


        internal void StartBackgroundPollLoop(Action<int> pollMethod)
        {
            if (consumerCts != null)
            {
                throw new Exception("Consumer background poll loop cannot be started twice.");
            }

            consumerCts = new CancellationTokenSource();
            var ct = consumerCts.Token;
            consumerTask = Task.Factory.StartNew(() =>
            {
                while (!ct.IsCancellationRequested)
                {
                    pollMethod(100);
                }
            }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        public void Start()
            => StartBackgroundPollLoop((Action<int>)Poll);

        public void Stop()
        {
            if (consumerCts == null)
            {
                throw new Exception("Consumer background poll loop not started - cannot stop.");
            }

            consumerCts.Cancel();
            consumerTask.Wait();
            consumerCts = null;
            consumerTask = null;
        }

        /// <summary>
        ///     Commit offsets for the current assignment.
        /// </summary>
        public void Commit()
            => kafkaHandle.Commit();

        /// <summary>
        ///     Commits an offset based on the topic/partition/offset of a message.
        ///     The next message to be read will be that following <param name="message" />.
        /// </summary>
        /// <remarks>
        ///     A consumer which has position N has consumed records with offsets 0 through N-1 and will next receive the record with offset N.
        ///     Hence, this method commits an offset of <param name="message">.Offset + 1.
        /// </remarks>
        public void Commit(Message message)
            => Commit(new List<TopicPartitionOffset> { new TopicPartitionOffset(message.TopicPartition, message.Offset + 1) });

        /// <summary>
        ///     Commit explicit list of offsets.
        /// </summary>
        public void Commit(ICollection<TopicPartitionOffset> offsets)
            => kafkaHandle.Commit(offsets);

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
        public List<TopicPartitionOffsetError> Committed(ICollection<TopicPartition> partitions, TimeSpan timeout)
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
        public List<TopicPartitionOffsetError> Position(ICollection<TopicPartition> partitions)
            => kafkaHandle.Position(partitions);

        public void Dispose()
        {
            kafkaHandle.ConsumerClose();
            kafkaHandle.Dispose();
        }

        public string Name
            => kafkaHandle.Name;

        public void Flush()
        {
            // TODO: implementation with rd_kafka_flush?
        }

        public string MemberId
            => kafkaHandle.MemberId;

        /// <summary>
        ///     The current librdkafka out queue length.
        /// </summary>
        /// <remarks>
        ///     The out queue contains messages and requests waiting to be sent to,
        ///     or acknowledged by, the broker.
        /// </summary>
        public long OutQueueLength
            => kafkaHandle.OutQueueLength;


        public List<GroupInfo> ListGroups(TimeSpan timeout)
            => kafkaHandle.ListGroups(timeout.TotalMillisecondsAsInt());

        // TODO: is a version of this with infinite timeout really required? (same question elsewhere)
        public List<GroupInfo> ListGroups()
            => kafkaHandle.ListGroups(-1);


        public GroupInfo ListGroup(string group, TimeSpan timeout)
            => kafkaHandle.ListGroup(group, timeout.TotalMillisecondsAsInt());

        public GroupInfo ListGroup(string group)
            => kafkaHandle.ListGroup(group, -1);


        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
            => kafkaHandle.GetWatermarkOffsets(topicPartition.Topic, topicPartition.Partition);


        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
            => kafkaHandle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, timeout.TotalMillisecondsAsInt());

        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition)
            => kafkaHandle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, -1);


        /// <summary>
        ///
        /// </summary>
        /// <param name="allTopics">
        ///     true - request all topics from cluster
        ///     false - request only locally known topics (topic_new():ed topics or otherwise locally referenced once, such as consumed topics)
        /// </param>
        /// <remarks>
        ///     TODO: Topic handles are not exposed to users of the library (they are internal to producer).
        ///           Is it possible to get a topic handle given a topic name?
        ///           If so, include topic parameter in this method.
        /// </remaarks>
        public Metadata GetMetadata(bool allTopics, TimeSpan timeout)
            => kafkaHandle.GetMetadata(allTopics, null, timeout.TotalMillisecondsAsInt());

        public Metadata GetMetadata(bool allTopics)
            => kafkaHandle.GetMetadata(allTopics, null, -1);
    }
}
