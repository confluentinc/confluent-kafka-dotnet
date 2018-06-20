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
    ///     Implements a high-level Apache Kafka consumer (without deserialization).
    /// </summary>
    internal class Consumer : IConsumer
    {
        /// <summary>
        ///     Name of the configuration property that specifies whether or not to
        ///     disable marshaling of headers when consuming messages. Note that 
        ///     disabling header marshaling will measurably improve maximum throughput 
        ///     even for the case where messages do not have any headers.
        /// 
        ///     default: true
        /// </summary>
        public const string EnableHeaderMarshalingPropertyName = "dotnet.consumer.enable.header.marshaling";

        private bool enableHeaderMarshaling = true;

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

            var modifiedConfig = config
                .Where(prop => prop.Key != EnableHeaderMarshalingPropertyName);

            var enableHeaderMarshalingObj = config.FirstOrDefault(prop => prop.Key == EnableHeaderMarshalingPropertyName).Value;
            if (enableHeaderMarshalingObj != null)
            {
                this.enableHeaderMarshaling = bool.Parse(enableHeaderMarshalingObj.ToString());
            }

            var configHandle = SafeConfigHandle.Create();
            modifiedConfig
                .ToList()
                .ForEach((kvp) => {
                    if (kvp.Value == null) throw new ArgumentException($"'{kvp.Key}' configuration parameter must not be null.");
                    configHandle.Set(kvp.Key, kvp.Value.ToString());
                });

            // Note: Setting default topic configuration properties via default.topic.config is depreciated 
            // and this functionality will be removed in a future version of the library.
            var defaultTopicConfig = (IEnumerable<KeyValuePair<string, object>>)config.FirstOrDefault(prop => prop.Key == "default.topic.config").Value;
            if (defaultTopicConfig != null)
            {
                defaultTopicConfig.ToList().ForEach(
                    (kvp) => {
                        if (kvp.Value == null) throw new ArgumentException($"'{kvp.Key}' configuration parameter in 'default.topic.config' must not be null.");
                        configHandle.Set(kvp.Key, kvp.Value.ToString());
                    }
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

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnPartitionsAssigned"]/*' />
        public event EventHandler<List<TopicPartition>> OnPartitionsAssigned;

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnPartitionsRevoked"]/*' />
        public event EventHandler<List<TopicPartition>> OnPartitionsRevoked;

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnOffsetsCommitted"]/*' />
        public event EventHandler<CommittedOffsets> OnOffsetsCommitted;

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnError"]/*' />
        public event EventHandler<Error> OnError;

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnConsumeError"]/*' />
        public event EventHandler<ConsumerRecord> OnConsumeError;

        /// <include file='include_docs_client.xml' path='API/Member[@name="OnStatistics"]/*' />
        public event EventHandler<string> OnStatistics;

        /// <include file='include_docs_client.xml' path='API/Member[@name="OnLog"]/*' />
        public event EventHandler<LogMessage> OnLog;

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnRecord"]/*' />
        public event EventHandler<ConsumerRecord> OnRecord;

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnPartitionEOF"]/*' />
        public event EventHandler<TopicPartitionOffset> OnPartitionEOF;

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Assignment"]/*' />
        public List<TopicPartition> Assignment
            => kafkaHandle.GetAssignment();

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Subscription"]/*' />
        public List<string> Subscription
            => kafkaHandle.GetSubscription();

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Subscribe_IEnumerable"]/*' />
        public void Subscribe(IEnumerable<string> topics)
            => kafkaHandle.Subscribe(topics);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Subscribe_string"]/*' />
        public void Subscribe(string topic)
            => Subscribe(new[] { topic });

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Unsubscribe"]/*' />
        public void Unsubscribe()
            => kafkaHandle.Unsubscribe();

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Assign_TopicPartition"]/*' />
        public void Assign(TopicPartition partition)
            => this.Assign(new List<TopicPartition> { partition });

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Assign_TopicPartitionOffset"]/*' />
        public void Assign(TopicPartitionOffset partition)
            => this.Assign(new List<TopicPartitionOffset> { partition });

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Assign_IEnumerable_TopicPartitionOffset"]/*' />
        public void Assign(IEnumerable<TopicPartitionOffset> partitions)
            => kafkaHandle.Assign(partitions.ToList());

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Assign_IEnumerable_TopicPartition"]/*' />
        public void Assign(IEnumerable<TopicPartition> partitions)
            => kafkaHandle.Assign(partitions.Select(p => new TopicPartitionOffset(p, Offset.Invalid)).ToList());

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Unassign"]/*' />
        public void Unassign()
            => kafkaHandle.Assign(null);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Consume_ConsumerRecord"]/*' />
        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Consume_ConsumerRecord_int"]/*' />
        public bool Consume(out ConsumerRecord record, int millisecondsTimeout)
        {
            if (kafkaHandle.ConsumerPoll(out record, enableHeaderMarshaling, (IntPtr)millisecondsTimeout))
            {
                switch (record.Error.Code)
                {
                    case ErrorCode.NoError:
                        return true;
                    case ErrorCode.Local_PartitionEOF:
                        OnPartitionEOF?.Invoke(this, record.TopicPartitionOffset);
                        return false;
                    default:
                        OnConsumeError?.Invoke(this, record);
                        return false;
                }
            }

            return false;
        }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Consume_ConsumerRecord"]/*' />
        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Consume_ConsumerRecord_TimeSpan"]/*' />
        public bool Consume(out ConsumerRecord record, TimeSpan timeout)
            => Consume(out record, timeout.TotalMillisecondsAsInt());

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Poll_TimeSpan"]/*' />
        public void Poll(TimeSpan timeout)
        {
            ConsumerRecord record;
            if (Consume(out record, timeout))
            {
                OnRecord?.Invoke(this, record);
            }
        }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Poll_int"]/*' />
        public void Poll(int millisecondsTimeout)
        {
            ConsumerRecord record;
            if (Consume(out record, millisecondsTimeout))
            {
                OnRecord?.Invoke(this, record);
            }
        }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="StoreOffset_ConsumerRecord"]/*' />
        public TopicPartitionOffsetError StoreOffset(ConsumerRecord record)
            => StoreOffsets(new[] { new TopicPartitionOffset(record.TopicPartition, record.Offset + 1) })[0];

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="StoreOffsets"]/*' />
        public List<TopicPartitionOffsetError> StoreOffsets(IEnumerable<TopicPartitionOffset> offsets)
            => kafkaHandle.StoreOffsets(offsets);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Commit"]/*' />
        public CommittedOffsets Commit()
            => kafkaHandle.Commit();
        
        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Commit_ConsumerRecord"]/*' />
        public CommittedOffsets Commit(ConsumerRecord record)
        {
            if (record.Error.Code != ErrorCode.NoError)
            {
                throw new InvalidOperationException("Attempt was made to commit offset corresponding to an errored message");
            }
            return Commit(new[] { new TopicPartitionOffset(record.TopicPartition, record.Offset + 1) });
        }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Commit_IEnumerable"]/*' />
        public CommittedOffsets Commit(IEnumerable<TopicPartitionOffset> offsets)
            => kafkaHandle.Commit(offsets);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Seek"]/*' />
        public void Seek(TopicPartitionOffset tpo)
            => kafkaHandle.Seek(tpo.Topic, tpo.Partition, tpo.Offset, -1);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Pause"]/*' />
        public List<TopicPartitionError> Pause(IEnumerable<TopicPartition> partitions)
            => kafkaHandle.Pause(partitions);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Resume"]/*' />
        public List<TopicPartitionError> Resume(IEnumerable<TopicPartition> partitions)
            => kafkaHandle.Resume(partitions);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Committed_IEnumerable_TimeSpan"]/*' />
        public List<TopicPartitionOffsetError> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
            => kafkaHandle.Committed(partitions, (IntPtr) timeout.TotalMillisecondsAsInt());

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Position_IEnumerable"]/*' />
        public List<TopicPartitionOffsetError> Position(IEnumerable<TopicPartition> partitions)
            => kafkaHandle.Position(partitions);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Dispose"]/*' />
        public void Dispose()
        {
            // note: consumers always own their handles.
            kafkaHandle.ConsumerClose();
            kafkaHandle.Dispose();
        }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OffsetsForTimes"]/*' />
        public IEnumerable<TopicPartitionOffsetError> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout)
            => kafkaHandle.OffsetsForTimes(timestampsToSearch, timeout.TotalMillisecondsAsInt());

        /// <include file='include_docs_client.xml' path='API/Member[@name="AddBrokers_string"]/*' />
        public int AddBrokers(string brokers)
            => kafkaHandle.AddBrokers(brokers);

        /// <include file='include_docs_client.xml' path='API/Member[@name="Client_Name"]/*' />
        public string Name
            => kafkaHandle.Name;

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="MemberId"]/*' />
        public string MemberId
            => kafkaHandle.MemberId;

        /// <summary>
        ///     An opaque reference to the underlying librdkafka client instance.
        /// </summary>
        public Handle Handle 
            => new Handle { Owner = this, LibrdkafkaHandle = kafkaHandle };
    }
}
