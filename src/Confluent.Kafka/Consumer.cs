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
                // TKey == Null -> cast always valid.
                KeyDeserializer = (IDeserializer<TKey>)new NullDeserializer();
            }

            if (ValueDeserializer == null)
            {
                if (typeof(TValue) != typeof(Null))
                {
                    throw new ArgumentNullException("Value deserializer must be specified.");
                }
                // TValue == Null -> cast always valid.
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
                new MessageInfo<TKey, TValue>
                {
                    Topic = e.Topic,
                    Partition = e.Partition,
                    Offset = e.Offset,
                    Key = KeyDeserializer.Deserialize(e.Key),
                    Value = ValueDeserializer.Deserialize(e.Value),
                    Timestamp = e.Timestamp,
                    Error = e.Error
                });
            consumer.OnPartitionEOF += (sender, e) => OnPartitionEOF?.Invoke(sender, e);
        }

        public MessageInfo<TKey, TValue>? Consume(TimeSpan? timeout = null)
        {
            // TODO: kafkaHandle on Consumer could be made internal, and we could potentially
            //       bypass the non-desrializing consumer completely here for efficiency.

            var result = consumer.Consume(timeout);
            if (!result.HasValue)
            {
                return null;
            }

            var message = result.Value;

            return new MessageInfo<TKey, TValue>
            {
                Topic = message.Topic,
                Partition = message.Partition,
                Offset = message.Offset,
                Key = KeyDeserializer.Deserialize(message.Key),
                Value = ValueDeserializer.Deserialize(message.Value),
                Timestamp = message.Timestamp,
                Error = message.Error
            };
        }

        public void Poll(TimeSpan? timeout = null)
        {
            var msg = Consume(timeout);
            if (msg != null)
            {
                OnMessage?.Invoke(this, msg.Value);
            }
        }

        public event EventHandler<List<TopicPartition>> OnPartitionsAssigned;

        public event EventHandler<List<TopicPartition>> OnPartitionsRevoked;

        public event EventHandler<OffsetCommitArgs> OnOffsetCommit;

        public event EventHandler<LogArgs> OnLog;

        public event EventHandler<string> OnStatistics;

        public event EventHandler<ErrorArgs> OnError;

        public event EventHandler<MessageInfo<TKey, TValue>> OnMessage;

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
            => consumer.Assign(null);

        /// <summary>
        ///     Commit offsets for the current assignment.
        /// </summary>
        public void Commit()
            => consumer.Commit();

        /// <summary>
        ///     Commit offset for specific topic/partition.
        /// </summary>
        public void Commit(MessageInfo<TKey, TValue> message)
            => consumer.Commit(new List<TopicPartitionOffset> { message.TopicPartitionOffset });

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
        ///     throws RdKafkaException if there was a problem retrieving the above information.
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
        ///     throws RdKafkaException if there was a problem retrieving the above information.
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

        public List<GroupInfo> ListGroups(TimeSpan? timeout = null)
            => consumer.ListGroups(timeout);

        public GroupInfo ListGroup(string group, TimeSpan? timeout = null)
            => consumer.ListGroup(group, timeout);

        public Offsets GetWatermarkOffsets(TopicPartition topicPartition)
            => consumer.GetWatermarkOffsets(topicPartition);

        public Offsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan? timeout = null)
            => consumer.QueryWatermarkOffsets(topicPartition, timeout);

        public Metadata GetMetadata(bool allTopics, TimeSpan? timeout = null)
            => consumer.GetMetadata(allTopics, timeout);
    }

    public class Consumer : IDisposable
    {
        private SafeKafkaHandle kafkaHandle;

        private void ErrorCallback(IntPtr rk, ErrorCode err, string reason, IntPtr opaque)
        {
            OnError?.Invoke(this, new ErrorArgs() { ErrorCode = err, Reason = reason });
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
                Loggers.ConsoleLogger(this, new LogArgs() { Name = name, Level = level, Facility = fac, Message = buf });
                return;
            }

            OnLog?.Invoke(this, new LogArgs() { Name = name, Level = level, Facility = fac, Message = buf });
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
            OnOffsetCommit?.Invoke(this, new OffsetCommitArgs()
                {
                    Error = err,
                    // TODO: check to see whether errors can ever be present here. If so, expose TPOE, not TPO.
                    Offsets = SafeKafkaHandle.GetTopicPartitionOffsetErrorList(offsets).Select(tp => tp.TopicPartitionOffset).ToList()
                });
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
            // TODO: figure out what Dup is exactly and if we need it.
            IntPtr cfgPtr = configHandle.Dup();

            rebalanceDelegate = RebalanceCallback;
            commitDelegate = CommitCallback;

            LibRdKafka.conf_set_rebalance_cb(cfgPtr, rebalanceDelegate);
            LibRdKafka.conf_set_offset_commit_cb(cfgPtr, commitDelegate);
            if (defaultTopicConfig != null)
            {
                var topicHandle = SafeTopicConfigHandle.Create();
                if (config != null)
                {
                    defaultTopicConfig.ToList().ForEach((kvp) => { topicHandle.Set(kvp.Key, kvp.Value.ToString()); });
                }
                LibRdKafka.conf_set_default_topic_conf(cfgPtr, topicHandle.Dup());
            }

            LibRdKafka.conf_set_error_cb(cfgPtr, ErrorCallback);
            LibRdKafka.conf_set_log_cb(cfgPtr, LogCallback);
            LibRdKafka.conf_set_stats_cb(cfgPtr, StatsCallback);

            this.kafkaHandle = SafeKafkaHandle.Create(RdKafkaType.Consumer, cfgPtr);
        }

        /// <summary>
        ///     Raised on new partition assignment.
        /// </summary>
        /// <remarks>
        ///     Typically call Assign() method.
        ///     TODO: entire assignment, or only new partitions?
        ///     On poll thread
        /// </remarks>
        public event EventHandler<List<TopicPartition>> OnPartitionsAssigned;

        /// <summary>
        ///     Raised when a partition assignment is revoked.
        /// </summary>
        /// <remarks>
        ///     Typically call Unassign() method
        ///     TODO: always everything unassigned? Reassignment
        ///     On poll thread.
        /// </remarks>
        public event EventHandler<List<TopicPartition>> OnPartitionsRevoked;

        /// <summary>
        ///     TODO: when exactly is this called?
        ///     I belive this is only called
        ///     On poll thread.
        /// </summary>
        public event EventHandler<OffsetCommitArgs> OnOffsetCommit;

        /// <summary>
        ///     Raised on critical errors, e.g. connection failures or all brokers down.
        ///     On poll thread.
        /// </summary>
        public event EventHandler<ErrorArgs> OnError;

        /// <summary>
        ///     Raised on librdkafka stats events.
        /// </summary>
        /// <remarks>
        ///     librdkafka statistics can be set using the statistics.interval.ms parameter.
        /// </remarks>
        public event EventHandler<string> OnStatistics;

        /// <summary>
        ///     Raised when there is information that should be to be logged.
        /// </summary>
        /// <remarks>
        ///     Specify which log level with the log_level configuration property.
        ///     Potentially on internal librdkafka thread. don't call librdkafka methods.
        /// </remarks>
        public event EventHandler<LogArgs> OnLog;

        /// <summary>
        ///
        /// </summary>
        /// <remarks>
        ///
        /// </remarks>
        public event EventHandler<MessageInfo> OnMessage;

        /// <summary>
        ///
        /// </summary>
        public event EventHandler<TopicPartitionOffset> OnPartitionEOF;


        /// <summary>
        ///     Returns the current partition assignment as set by Assign.
        /// </summary>
        public List<TopicPartition> Assignment
            => kafkaHandle.GetAssignment();

        /// <summary>
        ///     Returns the current partition subscription as set by Subscribe.
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
        public MessageInfo? Consume(TimeSpan? timeout = null)
        {
            int timeoutMs = -1;
            if (timeout.HasValue)
            {
                // TODO: is it worth having this check? It's on *EVERY* consume?
                // TODO: also investigate perf improvement if this method takes an int c.f. Timespan?
                timeoutMs = int.MaxValue;
                double _timeoutMs = (double)timeout.Value.TotalMilliseconds;
                if (_timeoutMs < int.MaxValue)
                {
                    timeoutMs = (int)_timeoutMs;
                }
            }

            var pollResult = kafkaHandle.ConsumerPoll((IntPtr)timeoutMs);

            if (pollResult.HasValue)
            {
                switch (pollResult.Value.Error.Code)
                {
                    case ErrorCode.NO_ERROR:
                        return pollResult;
                    case ErrorCode._PARTITION_EOF:
                        OnPartitionEOF?.Invoke(this, pollResult.Value.TopicPartitionOffset);
                        return null;
                    default:
                        OnError?.Invoke(this, new ErrorArgs { ErrorCode = pollResult.Value.Error.Code, Reason = null });
                        return null;
                }
            }

            return null;
        }

        public void Poll(TimeSpan? timeout)
        {
            var msg = Consume(timeout);
            if (msg != null)
            {
                OnMessage?.Invoke(this, msg.Value);
            }
        }

        /// <summary>
        ///     Commit offsets for the current assignment.
        /// </summary>
        public void Commit()
        {
            kafkaHandle.Commit();
        }

        /// <summary>
        ///     Commits an offset based on the topic/partition/offset of a message.
        ///     The next message to be read will be that following <param name="message" />.
        /// </summary>
        /// <remarks>
        ///     A consumer which has position N has consumed records with offsets 0 through N-1 and will next receive the record with offset N.
        ///     Hence, this method commits an offset of <param name="message">.Offset + 1.
        /// </remarks>
        public void Commit(MessageInfo message)
        {
            Commit(new List<TopicPartitionOffset> { new TopicPartitionOffset(message.TopicPartition, message.Offset + 1) });
        }

        /// <summary>
        ///     Commit explicit list of offsets.
        /// </summary>
        public void Commit(ICollection<TopicPartitionOffset> offsets)
        {
            kafkaHandle.Commit(offsets);
        }

        /// <summary>
        ///     Retrieve current committed offsets for topics + partitions.
        ///
        ///     The offset field of each requested partition will be set to the offset
        ///     of the last consumed message, or RD_KAFKA_OFFSET_INVALID in case there was
        ///     no previous message, or, alternately a partition specific error may also be
        ///     returned.
        ///
        ///     throws RdKafkaException if there was a problem retrieving the above information.
        /// </summary>
        public List<TopicPartitionOffsetError> Committed(ICollection<TopicPartition> partitions, TimeSpan timeout)
            => kafkaHandle.Committed(partitions, (IntPtr) timeout.TotalMilliseconds);

        /// <summary>
        ///     Retrieve current positions (offsets) for topics + partitions.
        ///
        ///     The offset field of each requested partition will be set to the offset
        ///     of the last consumed message + 1, or RD_KAFKA_OFFSET_INVALID in case there was
        ///     no previous message, or, alternately a partition specific error may also be
        ///     returned.
        ///
        ///     throws RdKafkaException if there was a problem retrieving the above information.
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


        public List<GroupInfo> ListGroups(TimeSpan? timeout = null)
            => kafkaHandle.ListGroups(timeout);

        public GroupInfo ListGroup(string group, TimeSpan? timeout = null)
            => kafkaHandle.ListGroup(group, timeout);

        public Offsets GetWatermarkOffsets(TopicPartition topicPartition)
            => kafkaHandle.GetWatermarkOffsets(topicPartition.Topic, topicPartition.Partition);

        public Offsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan? timeout = null)
            => kafkaHandle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, timeout);

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
        public Metadata GetMetadata(bool allTopics, TimeSpan? timeout = null)
            => kafkaHandle.GetMetadata(allTopics, null, timeout);

    }
}
