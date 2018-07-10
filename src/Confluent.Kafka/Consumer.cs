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
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Serialization;
using System.Runtime.InteropServices;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Implements a high-level Apache Kafka consumer (with 
    ///     key and value deserialization).
    /// </summary>
    public class Consumer<TKey, TValue> : IConsumer<TKey, TValue>
    {
        /// <summary>
        ///     Name of the configuration property that specifies whether or not to
        ///     enable marshaling of headers when consuming messages. Note that 
        ///     disabling header marshaling will measurably improve maximum throughput 
        ///     even for the case where messages do not have any headers.
        /// 
        ///     default: true
        /// </summary>
        public const string EnableHeadersPropertyName = "dotnet.consumer.enable.headers";

        /// <summary>
        ///     Name of the configuration property that specifies whether or not to
        ///     enable marshaling of timestamps when consuming messages. Disabling this
        ///     will improve maximum throughput.
        /// </summary>
        public const string EnableTimestampPropertyName = "dotnet.consumer.enable.timestamps";

        /// <summary>
        ///     Name of the configuration property that specifies whether or not to
        ///     enable marshaling of topic names when consuming messages. Disabling
        ///     this will improve maximum throughput.
        /// </summary>
        public const string EnableTopicNamesPropertyName = "dotnet.consumer.enable.topic.names";


        private readonly bool enableHeaderMarshaling = true;
        private readonly bool enableTimestampMarshaling = true;
        private readonly bool enableTopicNamesMarshaling = true;

        private readonly SafeKafkaHandle kafkaHandle;

        private static readonly byte[] EmptyBytes = new byte[0];

        private readonly Librdkafka.ErrorDelegate errorDelegate;
        private void ErrorCallback(IntPtr rk, ErrorCode err, string reason, IntPtr opaque)
        {
            OnError?.Invoke(this, new Error(err, reason));
        }

        private readonly Librdkafka.StatsDelegate statsDelegate;
        private int StatsCallback(IntPtr rk, IntPtr json, UIntPtr json_len, IntPtr opaque)
        {
            OnStatistics?.Invoke(this, Util.Marshal.PtrToStringUTF8(json));
            return 0; // instruct librdkafka to immediately free the json ptr.
        }

        private readonly Librdkafka.LogDelegate logDelegate;
        private void LogCallback(IntPtr rk, SyslogLevel level, string fac, string buf)
        {
            var name = Util.Marshal.PtrToStringUTF8(Librdkafka.name(rk));

            if (OnLog == null)
            {
                // A stderr logger is used by default if none is specified.
                Loggers.ConsoleLogger(this, new LogMessage(name, level, fac, buf));
                return;
            }

            OnLog?.Invoke(this, new LogMessage(name, level, fac, buf));
        }

        private readonly Librdkafka.RebalanceDelegate rebalanceDelegate;
        private void RebalanceCallback(
            IntPtr rk,
            ErrorCode err,
            IntPtr partitions,
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

        private readonly Librdkafka.CommitDelegate commitDelegate;
        private void CommitCallback(
            IntPtr rk,
            ErrorCode err,
            IntPtr offsets,
            IntPtr opaque)
        {
            OnOffsetsCommitted?.Invoke(this, new CommittedOffsets(
                SafeKafkaHandle.GetTopicPartitionOffsetErrorList(offsets),
                new Error(err)
            ));
        }


        /// <include file='include_docs_consumer.xml' path='API/Member[@name="KeyDeserializer"]/*' />
        public IDeserializer<TKey> KeyDeserializer { get; }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="ValueDeserializer"]/*' />
        public IDeserializer<TValue> ValueDeserializer { get; }

        /// <summary>
        ///     Creates a new Consumer instance.
        /// </summary>
        /// <param name="config">
        ///     librdkafka configuration parameters 
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
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
            Librdkafka.Initialize(null);

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

            config = null;

            if (configWithoutDeserializerProperties.FirstOrDefault(prop => string.Equals(prop.Key, "group.id", StringComparison.Ordinal)).Value == null)
            {
                throw new ArgumentException("'group.id' configuration parameter is required and was not specified.");
            }

            var modifiedConfig = configWithoutDeserializerProperties
                .Where(prop => 
                    prop.Key != EnableHeadersPropertyName &&
                    prop.Key != EnableTimestampPropertyName &&
                    prop.Key != EnableTopicNamesPropertyName);

            var enableHeaderMarshalingObj = configWithoutDeserializerProperties.FirstOrDefault(prop => prop.Key == EnableHeadersPropertyName).Value;
            if (enableHeaderMarshalingObj != null)
            {
                this.enableHeaderMarshaling = bool.Parse(enableHeaderMarshalingObj.ToString());
            }

            var enableTimestampMarshalingObj = configWithoutDeserializerProperties.FirstOrDefault(prop => prop.Key == EnableTimestampPropertyName).Value;
            if (enableTimestampMarshalingObj != null)
            {
                this.enableTimestampMarshaling = bool.Parse(enableTimestampMarshalingObj.ToString());
            }

            var enableTopicNamesMarshalingObj = configWithoutDeserializerProperties.FirstOrDefault(prop => prop.Key == EnableTopicNamesPropertyName).Value;
            if (enableTopicNamesMarshalingObj != null)
            {
                this.enableTopicNamesMarshaling = bool.Parse(enableTopicNamesMarshalingObj.ToString());
            }

            var configHandle = SafeConfigHandle.Create();
            modifiedConfig
                .ToList()
                .ForEach((kvp) => {
                    if (kvp.Value == null) throw new ArgumentException($"'{kvp.Key}' configuration parameter must not be null.");
                    configHandle.Set(kvp.Key, kvp.Value.ToString());
                });

            // Explicitly keep references to delegates so they are not reclaimed by the GC.
            rebalanceDelegate = RebalanceCallback;
            commitDelegate = CommitCallback;
            errorDelegate = ErrorCallback;
            logDelegate = LogCallback;
            statsDelegate = StatsCallback;

            IntPtr configPtr = configHandle.DangerousGetHandle();

            Librdkafka.conf_set_rebalance_cb(configPtr, rebalanceDelegate);
            Librdkafka.conf_set_offset_commit_cb(configPtr, commitDelegate);

            Librdkafka.conf_set_error_cb(configPtr, errorDelegate);
            Librdkafka.conf_set_log_cb(configPtr, logDelegate);
            Librdkafka.conf_set_stats_cb(configPtr, statsDelegate);

            this.kafkaHandle = SafeKafkaHandle.Create(RdKafkaType.Consumer, configPtr);
            configHandle.SetHandleAsInvalid(); // config object is no longer useable.

            var pollSetConsumerError = kafkaHandle.PollSetConsumer();
            if (pollSetConsumerError != ErrorCode.NoError)
            {
                throw new KafkaException(new Error(pollSetConsumerError,
                    $"Failed to redirect the poll queue to consumer_poll queue: {ErrorCodeExtensions.GetReason(pollSetConsumerError)}"));
            }
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

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Consume_ConsumerRecord"]/*' />
        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Consume_ConsumerRecord_int"]/*' />
        public bool Consume(out ConsumerRecord<TKey, TValue> record, int millisecondsTimeout)
        {
            var msgPtr = kafkaHandle.ConsumerPoll(enableTimestampMarshaling, enableHeaderMarshaling, (IntPtr)millisecondsTimeout);
            if (msgPtr == IntPtr.Zero)
            {
                record = null;
                return false;
            }

            try
            {
                var msg = Util.Marshal.PtrToStructureUnsafe<rd_kafka_message>(msgPtr);

                string topic = null;
                if (this.enableTopicNamesMarshaling)
                {
                    if (msg.rkt != IntPtr.Zero)
                    {
                        topic = Util.Marshal.PtrToStringUTF8(Librdkafka.topic_name(msg.rkt));
                    }
                }

                if (msg.err == ErrorCode.Local_PartitionEOF)
                {
                    record = null;
                    OnPartitionEOF?.Invoke(this, new TopicPartitionOffset(topic, msg.partition, msg.offset));
                    return false;
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
                            var headerValue = new byte[(int)sizep];
                            Marshal.Copy(valuep, headerValue, 0, (int)sizep);
                            headers.Add(new Header(headerName, headerValue));
                        }
                    }
                }

                if (msg.err != ErrorCode.NoError)
                {
                    throw new ConsumeException(
                        new ConsumerRecord<byte[], byte[]>
                        {
                            TopicPartitionOffsetError = new TopicPartitionOffsetError(topic, msg.partition, msg.offset, new Error(msg.err)),
                            Message = new Message<byte[], byte[]>
                            {
                                Timestamp = timestamp,
                                Headers = headers,
                                Key = KeyAsByteArray(msg),
                                Value = ValueAsByteArray(msg)
                            }
                        }
                    );
                }

                TKey key;
                try
                {
                    unsafe
                    {
                        key = this.KeyDeserializer.Deserialize(
                            topic,
                            msg.key == IntPtr.Zero ? EmptyBytes : new ReadOnlySpan<byte>(msg.key.ToPointer(), (int)msg.key_len),
                            msg.key == IntPtr.Zero
                        );
                    }
                }
                catch (Exception ex)
                {
                    throw new ConsumeException(
                        new ConsumerRecord<byte[], byte[]>
                        {
                            TopicPartitionOffsetError = new TopicPartitionOffsetError(topic, msg.partition, msg.offset, new Error(ErrorCode.Local_KeyDeserialization, ex.ToString())),
                            Message = new Message<byte[], byte[]>
                            {
                                Timestamp = timestamp,
                                Headers = headers,
                                Key = KeyAsByteArray(msg),
                                Value = ValueAsByteArray(msg)
                            }
                        }
                    );
                }

                TValue val;
                try
                {
                    unsafe
                    {
                        val = this.ValueDeserializer.Deserialize(
                            topic, 
                            msg.val == IntPtr.Zero ? EmptyBytes : new ReadOnlySpan<byte>(msg.val.ToPointer(), (int)msg.len),
                            msg.val == IntPtr.Zero);
                    }
                }
                catch (Exception ex)
                {
                    throw new ConsumeException(
                        new ConsumerRecord<byte[], byte[]>
                        {
                            TopicPartitionOffsetError = new TopicPartitionOffsetError(topic, msg.partition, msg.offset, new Error(ErrorCode.Local_ValueDeserialization, ex.ToString())),
                            Message = new Message<byte[], byte[]>
                            {
                                Timestamp = timestamp,
                                Headers = headers,
                                Key = KeyAsByteArray(msg),
                                Value = ValueAsByteArray(msg)
                            }
                        }
                    );
                }

                record = new ConsumerRecord<TKey, TValue> 
                { 
                    TopicPartitionOffsetError = new TopicPartitionOffsetError(topic, msg.partition, msg.offset, new Error(ErrorCode.NoError)),
                    Message = new Message<TKey, TValue>
                    {
                        Timestamp = timestamp,
                        Headers = headers,
                        Key = key,
                        Value = val
                    }
                };

                return true;
            }
            finally
            {
                Librdkafka.message_destroy(msgPtr);
            }
        }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Consume_ConsumerRecord"]/*' />
        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Consume_ConsumerRecord_TimeSpan"]/*' />
        public bool Consume(out ConsumerRecord<TKey, TValue> record, TimeSpan timeout)
            => Consume(out record, timeout.TotalMillisecondsAsInt());


        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Poll_int"]/*' />
        public void Poll(int millisecondsTimeout)
        {
            if (Consume(out var record, millisecondsTimeout))
            {
                OnRecord?.Invoke(this, record);
            }
        }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Poll_TimeSpan"]/*' />
        public void Poll(TimeSpan timeout)
        {
            if (Consume(out var record, timeout))
            {
                OnRecord?.Invoke(this, record);
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

        /// <include file='include_docs_client.xml' path='API/Member[@name="OnStatistics"]/*' />
        public event EventHandler<string> OnStatistics;

        /// <include file='include_docs_client.xml' path='API/Member[@name="OnLog"]/*' />
        public event EventHandler<LogMessage> OnLog;

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnPartitionEOF"]/*' />
        public event EventHandler<TopicPartitionOffset> OnPartitionEOF;


        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnMessage"]/*' />
        public event EventHandler<ConsumerRecord<TKey, TValue>> OnRecord;

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

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="StoreOffset_ConsumerRecord"]/*' />
        public TopicPartitionOffsetError StoreOffset(ConsumerRecord<TKey, TValue> record)
            => StoreOffsets(new[] { new TopicPartitionOffset(record.TopicPartition, record.Offset + 1) })[0];

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="StoreOffsets"]/*' />
        public List<TopicPartitionOffsetError> StoreOffsets(IEnumerable<TopicPartitionOffset> offsets)
            => kafkaHandle.StoreOffsets(offsets);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Commit"]/*' />
        public CommittedOffsets Commit()
            => kafkaHandle.Commit();

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Commit_ConsumerRecord"]/*' />
        public CommittedOffsets Commit(ConsumerRecord<TKey, TValue> record)
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

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Dispose"]/*' />
        public void Dispose()
        {
            // note: consumers always own their own handles.

            KeyDeserializer?.Dispose();
            ValueDeserializer?.Dispose();

            // note: consumers always own their handles.
            kafkaHandle.ConsumerClose();
            kafkaHandle.Dispose();
        }

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
