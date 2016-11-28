using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;


namespace Confluent.Kafka
{
    public class Consumer : IDisposable
    {
        Task consumerTask;
        CancellationTokenSource consumerCts;

        private SafeKafkaHandle kafkaHandle;

        private void ErrorCallback(IntPtr rk, ErrorCode err, string reason, IntPtr opaque)
        {
            OnError?.Invoke(this, new ErrorArgs() { ErrorCode = err, Reason = reason });
        }

        private int StatsCallback(IntPtr rk, IntPtr json, UIntPtr json_len, IntPtr opaque)
        {
            OnStatistics?.Invoke(this, Util.Marshal.PtrToStringUTF8(json));
            return 0; // TODO: What's this for?
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
            var partitionList = SafeKafkaHandle.GetTopicPartitionOffsetList(partitions);
            if (err == ErrorCode._ASSIGN_PARTITIONS)
            {
                var handler = OnPartitionsAssigned;
                if (handler != null && handler.GetInvocationList().Length > 0)
                {
                    handler(this, partitionList);
                }
                else
                {
                    Assign(partitionList);
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
                    Offsets = SafeKafkaHandle.GetTopicPartitionOffsetList(offsets)
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
        public event EventHandler<List<TopicPartitionOffset>> OnPartitionsAssigned;

        /// <summary>
        ///     Raised when a partition assignment is revoked.
        /// </summary>
        /// <remarks>
        ///     Typically call Unassign() method
        ///     TODO: always everything unassigned? Reassignment
        ///     On poll thread.
        /// </remarks>
        public event EventHandler<List<TopicPartitionOffset>> OnPartitionsRevoked;

        /// <summary>
        ///     TODO: when exactly is this called?
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
        public event EventHandler<TopicPartitionOffset> OnEndReached;


        /// <summary>
        ///     Returns the current partition assignment as set by Assign.
        /// </summary>
        public List<TopicPartition> Assignment => kafkaHandle.GetAssignment();

        /// <summary>
        ///     Returns the current partition subscription as set by Subscribe.
        /// </summary>
        public List<string> Subscription => kafkaHandle.GetSubscription();

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
        public void Subscribe(ICollection<string> topics) => kafkaHandle.Subscribe(topics);

        /// <summary>
        ///     Unsubscribe from the current subscription set.
        /// </summary>
        public void Unsubscribe() => kafkaHandle.Unsubscribe();

        /// <summary>
        ///     Update the assignment set to \p partitions.
        ///
        ///     The assignment set is the set of partitions actually being consumed
        ///     by the KafkaConsumer.
        /// </summary>
        /// <remarks>
        ///     blocks
        /// </remarks>
        public void Assign(ICollection<TopicPartitionOffset> partitions) => kafkaHandle.Assign(partitions);

        /// <summary>
        ///     Stop consumption and remove the current assignment.
        /// </summary>
        public void Unassign() => kafkaHandle.Assign(null);

        /// <summary>
        ///     Manually consume message or triggers events.
        ///
        ///     Will invoke events for OnPartitionsAssigned/Revoked,
        ///     OnOffsetCommit, etc. on the calling thread.
        /// </summary>
        public MessageInfo? Consume(TimeSpan timeout)
        {
            var consumerPollResult = kafkaHandle.ConsumerPoll((IntPtr)timeout.TotalMilliseconds);

            if (consumerPollResult != null)
            {
                var cpr = consumerPollResult.Value;

                if (cpr.ErrorCode == ErrorCode.NO_ERROR)
                {
                    return new MessageInfo()
                    {
                        Topic = cpr.Topic,
                        Partition = cpr.Partition,
                        Offset = cpr.Offset,
                        Value = cpr.Value,
                        Key = cpr.Key
                    };
                }

                else if (cpr.ErrorCode == ErrorCode._PARTITION_EOF)
                {
                    OnEndReached?.Invoke(
                        this,
                        new TopicPartitionOffset()
                        {
                            Topic = cpr.Topic,
                            Partition = cpr.Partition,
                            Offset = cpr.Offset,
                        });
                }

                else
                {
                    OnError?.Invoke(this, new ErrorArgs { ErrorCode = cpr.ErrorCode, Reason = null });
                }
            }

            return null;
        }

        public void Start()
        {
            if (consumerTask != null)
            {
                throw new InvalidOperationException("Consumer task already running");
            }

            consumerCts = new CancellationTokenSource();
            var ct = consumerCts.Token;
            consumerTask = Task.Factory.StartNew(() =>
                {
                    while (!ct.IsCancellationRequested)
                    {
                        var msg = Consume(TimeSpan.FromSeconds(1));
                        if (msg != null)
                        {
                            OnMessage?.Invoke(this, msg.Value);
                        }
                    }
                }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        public async Task Stop()
        {
            consumerCts.Cancel();
            try
            {
                await consumerTask;
            }
            finally
            {
                consumerTask = null;
                consumerCts = null;
            }
        }

        /// <summary>
        ///     Commit offsets for the current assignment.
        /// </summary>
        public Task Commit()
        {
            kafkaHandle.Commit();
            return Task.FromResult(false);
        }

        /// <summary>
        ///     Commit offset for a single topic+partition based on message.
        /// </summary>
        public Task Commit(MessageInfo message)
        {
            var tpo = message.TopicPartitionOffset;
            Commit(new List<TopicPartitionOffset>()
                   {
                       new TopicPartitionOffset(tpo.Topic, tpo.Partition, tpo.Offset + 1)
                   });
            return Task.FromResult(false);
        }

        /// <summary>
        ///     Commit explicit list of offsets.
        /// </summary>
        public Task Commit(ICollection<TopicPartitionOffset> offsets)
        {
            kafkaHandle.Commit(offsets);
            return Task.FromResult(false);
        }

        /// <summary>
        ///     Retrieve committed offsets for topics+partitions.
        /// </summary>
        public Task<List<TopicPartitionOffset>> Committed(ICollection<TopicPartition> partitions, TimeSpan timeout)
        {
            var result = kafkaHandle.Committed(partitions, (IntPtr) timeout.TotalMilliseconds);
            return Task.FromResult(result);
        }

        /// <summary>
        ///     Retrieve current positions (offsets) for topics+partitions.
        ///
        ///     The offset field of each requested partition will be set to the offset
        ///     of the last consumed message + 1, or RD_KAFKA_OFFSET_INVALID in case there was
        ///     no previous message.
        /// </summary>
        public List<TopicPartitionOffset> Position(ICollection<TopicPartition> partitions)
            => kafkaHandle.Position(partitions);

        public void Dispose()
        {
            kafkaHandle.ConsumerClose();
            kafkaHandle.Dispose();
        }

        public string Name => kafkaHandle.Name;

        public void Flush()
        {
            // implementation?
            // rd_kafka_flush
        }

        public string MemberId => kafkaHandle.MemberId;

        /// <summary>
        ///     The current librdkafka out queue length.
        /// </summary>
        /// <remarks>
        ///     The out queue contains messages and requests waiting to be sent to,
        ///     or acknowledged by, the broker.
        /// </summary>
        public long OutQueueLength => kafkaHandle.OutQueueLength;


        public List<GroupInfo> ListGroups(TimeSpan? timeout = null)
            => kafkaHandle.ListGroups(timeout);

        public GroupInfo ListGroup(string group, TimeSpan? timeout = null)
            => kafkaHandle.ListGroup(group, timeout);

        public Offsets GetWatermarkOffsets(TopicPartition topicPartition)
            => kafkaHandle.GetWatermarkOffsets(topicPartition.Topic, topicPartition.Partition);

        public Offsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan? timeout = null)
            => kafkaHandle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, timeout);

        public Metadata GetMetadata(string topic = null, bool includeInternal = false, TimeSpan? timeout = null)
        {
            if (topic != null)
            {
                // TODO: does it make sense to ever support this case on Consumer?
                throw new NotImplementedException("topic must be null when calling GetMetadataAsync on Consumer.");
            }
            return kafkaHandle.GetMetadata(null, includeInternal, timeout);
        }

    }
}
