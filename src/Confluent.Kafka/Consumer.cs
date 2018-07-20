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
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
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
    public class Consumer<TKey, TValue> : IConsumer<TKey, TValue>
    {
        private readonly bool enableHeaderMarshaling = true;
        private readonly bool enableTimestampMarshaling = true;
        private readonly bool enableTopicNamesMarshaling = true;

        private IDeserializer<TKey> KeyDeserializer { get; }
        private IDeserializer<TValue> ValueDeserializer { get; }

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

        private Action<LogMessage> logDelegate;
        private readonly Librdkafka.LogDelegate logCallbackDelegate;
        private void LogCallback(IntPtr rk, SyslogLevel level, string fac, string buf)
        {
            var name = Util.Marshal.PtrToStringUTF8(Librdkafka.name(rk));

            if (logDelegate == null)
            {
                // A stderr logger is used by default if none is specified.
                Loggers.ConsoleLogger(this, new LogMessage(name, level, fac, buf));
                return;
            }

            logDelegate(new LogMessage(name, level, fac, buf));
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
                    prop.Key != ConfigPropertyNames.EnableHeadersPropertyName &&
                    prop.Key != ConfigPropertyNames.EnableTimestampPropertyName &&
                    prop.Key != ConfigPropertyNames.EnableTopicNamesPropertyName &&
                    prop.Key != ConfigPropertyNames.LogDelegateName);

            var enableHeaderMarshalingObj = configWithoutDeserializerProperties.FirstOrDefault(prop => prop.Key == ConfigPropertyNames.EnableHeadersPropertyName).Value;
            if (enableHeaderMarshalingObj != null)
            {
                this.enableHeaderMarshaling = bool.Parse(enableHeaderMarshalingObj.ToString());
            }

            var enableTimestampMarshalingObj = configWithoutDeserializerProperties.FirstOrDefault(prop => prop.Key == ConfigPropertyNames.EnableTimestampPropertyName).Value;
            if (enableTimestampMarshalingObj != null)
            {
                this.enableTimestampMarshaling = bool.Parse(enableTimestampMarshalingObj.ToString());
            }

            var enableTopicNamesMarshalingObj = configWithoutDeserializerProperties.FirstOrDefault(prop => prop.Key == ConfigPropertyNames.EnableTopicNamesPropertyName).Value;
            if (enableTopicNamesMarshalingObj != null)
            {
                this.enableTopicNamesMarshaling = bool.Parse(enableTopicNamesMarshalingObj.ToString());
            }

            var logDelegateObj = configWithoutDeserializerProperties.FirstOrDefault(prop => prop.Key == ConfigPropertyNames.LogDelegateName).Value;
            if (logDelegateObj != null)
            {
                this.logDelegate = (Action<LogMessage>)logDelegateObj;
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
            logCallbackDelegate = LogCallback;
            statsDelegate = StatsCallback;

            IntPtr configPtr = configHandle.DangerousGetHandle();

            Librdkafka.conf_set_rebalance_cb(configPtr, rebalanceDelegate);
            Librdkafka.conf_set_offset_commit_cb(configPtr, commitDelegate);

            Librdkafka.conf_set_error_cb(configPtr, errorDelegate);
            Librdkafka.conf_set_log_cb(configPtr, logCallbackDelegate);
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

        /// <summary>
        ///     Poll for new messages / consumer events. Blocks until a new 
        ///     message or event is ready to be handled or the timeout period
        ///     has elapsed.
        /// </summary>
        /// <returns>
        ///     A consumed record, or null if no messages are 
        ///     available for consumption.
        ///     true: a message (with non-error state) was consumed.
        ///     false: no message was available for consumption.
        /// </returns>
        /// <remarks>
        ///     Will invoke events for OnPartitionsAssigned/Revoked,
        ///     OnOffsetsCommitted, OnConsumeError etc. on the calling 
        ///     thread.
        /// </remarks>
        internal ConsumeResult<TKey, TValue> Consume(int millisecondsTimeout)
        {
            var msgPtr = kafkaHandle.ConsumerPoll(enableTimestampMarshaling, enableHeaderMarshaling, (IntPtr)millisecondsTimeout);
            if (msgPtr == IntPtr.Zero)
            {
                return new ConsumeResult<TKey, TValue>
                {
                    TopicPartitionOffsetError = new TopicPartitionOffsetError(null, Partition.Any, Offset.Invalid, new Error(ErrorCode.Local_TimedOut)),
                    Message = null
                };
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
                    return new ConsumeResult<TKey, TValue>
                    {
                        TopicPartitionOffsetError = new TopicPartitionOffsetError(topic, msg.partition, msg.offset, new Error(msg.err))
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
                            var headerValue = new byte[(int)sizep];
                            Marshal.Copy(valuep, headerValue, 0, (int)sizep);
                            headers.Add(new Header(headerName, headerValue));
                        }
                    }
                }

                if (msg.err != ErrorCode.NoError)
                {
                    throw new ConsumeException(
                        new ConsumeResult<byte[], byte[]>
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
                        new ConsumeResult<byte[], byte[]>
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
                        new ConsumeResult<byte[], byte[]>
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

                return new ConsumeResult<TKey, TValue> 
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
            }
            finally
            {
                Librdkafka.message_destroy(msgPtr);
            }
        }


        public ConsumeResult<TKey, TValue> Consume(TimeSpan timeout)
            => Consume(timeout.TotalMillisecondsAsInt());


        public ConsumeResult<TKey, TValue> Consume(CancellationToken cancellationToken = default(CancellationToken))
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var result = Consume(100);

                switch(result.Error.Code)
                {
                    case ErrorCode.Local_TimedOut:
                        continue;
                    default:
                        return result;
                }
            }
        }


        public Task<ConsumeResult<TKey, TValue>> ConsumeAsync(TimeSpan timeout)
            => Task.Run(() => Consume(timeout));


        public Task<ConsumeResult<TKey, TValue>> ConsumeAsync(CancellationToken cancellationToken = default(CancellationToken))
            => Task.Run(() => Consume(cancellationToken));


        /// <summary>
        ///     Raised on new partition assignment.
        ///     You should typically call the Consumer.Assign method in this handler.
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler 
        ///     (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<List<TopicPartition>> OnPartitionsAssigned;


        /// <summary>
        ///     Raised when a partition assignment is revoked.
        ///     You should typically call the Consumer.Unassign method in this handler.
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler 
        ///     (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<List<TopicPartition>> OnPartitionsRevoked;


        /// <summary>
        ///     Raised to report the result of (automatic) offset commits.
        ///     Not raised as a result of the use of the Commit method.
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler 
        ///     (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<CommittedOffsets> OnOffsetsCommitted;


        /// <summary>
        ///     Raised on critical errors, e.g. connection failures or all 
        ///     brokers down. Note that the client will try to automatically 
        ///     recover from errors - these errors should be seen as 
        ///     informational rather than catastrophic
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event 
        ///     handler (except OnLog which may be called from an arbitrary thread).
        /// </remarks>
        public event EventHandler<Error> OnError;


        /// <summary>
        ///     Raised on librdkafka statistics events. JSON formatted
        ///     string as defined here: https://github.com/edenhill/librdkafka/wiki/Statistics
        /// </summary>
        /// <remarks>
        ///     You can enable statistics and set the statistics interval
        ///     using the statistics.interval.ms configuration parameter
        ///     (disabled by default).
        ///
        ///     Called as a side effect of calls to `Poll` (producer/consumer) or 
        ///     `Consume` (consumer), on the same thread.
        /// </remarks>
        public event EventHandler<string> OnStatistics;


        /// <summary>
        ///     Gets the current partition assignment as set by Assign.
        /// </summary>
        public List<TopicPartition> Assignment
            => kafkaHandle.GetAssignment();


        /// <summary>
        ///     Gets the current partition subscription as set by Subscribe.
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
        ///     according to the 'auto.offset.reset' configuration parameter if
        ///     no offsets have been committed yet.
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
        ///     Stop consumption and remove the current assignment.
        /// </summary>
        public void Unassign()
            => kafkaHandle.Assign(null);


        /// <summary>
        ///     Store offsets for a single partition based on the topic/partition/offset
        ///     of a consumer record.
        /// 
        ///     The offset will be committed (written) to the offset store according
        ///     to `auto.commit.interval.ms` or manual offset-less commit().
        /// </summary>
        /// <remarks>
        ///     `enable.auto.offset.store` must be set to "false" when using this API.
        /// </remarks>
        /// <param name="record">
        ///     A consumer record used to determine the offset to store and topic/partition.
        /// </param>
        /// <returns>
        ///     Current stored offset or a partition specific error.
        /// </returns>
        public TopicPartitionOffsetError StoreOffset(ConsumeResult<TKey, TValue> record)
            => StoreOffsets(new[] { new TopicPartitionOffset(record.TopicPartition, record.Offset + 1) })[0];


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
        public List<TopicPartitionOffsetError> StoreOffsets(IEnumerable<TopicPartitionOffset> offsets)
            => kafkaHandle.StoreOffsets(offsets);


        /// <summary>
        ///     Commit offsets for the current assignment.
        /// </summary>
        public Task<List<TopicPartitionOffsetError>> CommitAsync(CancellationToken cancellationToken = default(CancellationToken))
            => Task.Run(() => kafkaHandle.CommitSync(null));


        /// <summary>
        ///     Commits an offset based on the topic/partition/offset of a message.
        ///     The next message to be read will be that following <paramref name="record" />.
        /// </summary>
        /// <param name="record">
        ///     The ConsumerRecord instance used to determine the committed offset.
        /// </param>
        /// <remarks>
        ///     A consumer which has position N has consumed records with offsets 0 through N-1 
        ///     and will next receive the record with offset N. Hence, this method commits an 
        ///     offset of <paramref name="record" />.Offset + 1.
        /// </remarks>
        public Task<List<TopicPartitionOffsetError>> CommitAsync(ConsumeResult<TKey, TValue> record, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (record.Error.Code != ErrorCode.NoError)
            {
                throw new InvalidOperationException("Attempt was made to commit offset corresponding to an errored message");
            }

            return CommitAsync(new[] { new TopicPartitionOffset(record.TopicPartition, record.Offset + 1) }, cancellationToken);
        }


        /// <summary>
        ///     Commit an explicit list of offsets.
        /// </summary>
        /// <remarks>
        ///     Note: A consumer which has position N has consumed records with offsets 0 through N-1 
        ///     and will next receive the record with offset N.
        /// </remarks>
        public Task<List<TopicPartitionOffsetError>> CommitAsync(IEnumerable<TopicPartitionOffset> offsets, CancellationToken cancellationToken = default(CancellationToken))
            => Task.Run(() => kafkaHandle.CommitSync(offsets));


        /// <summary>
        ///     Seek consumer for topic+partition to <parmref name="offset"/> which is either an
        ///     absolute or logical offset. This must only be done for partitions that are 
        ///     currently being consumed (i.e., have been Assign()ed). To set the start offset for 
        ///     not-yet-consumed partitions you should use the 
        ///     <see cref="Confluent.Kafka.Consumer.Assign(IEnumerable{TopicPartitionOffset})" /> 
        ///     method instead.
        /// </summary>
        /// <param name="tpo">
        ///     The topic/partition to seek on and the offset to seek to.
        /// </param>
        public void Seek(TopicPartitionOffset tpo)
            => kafkaHandle.Seek(tpo.Topic, tpo.Partition, tpo.Offset, -1);


        /// <summary>
        ///     Pause consumption for the provided list of partitions.
        /// </summary>
        /// <param name="partitions">
        ///     The partitions to pause consumption of.
        /// </param>
        /// <returns>
        ///     Per partition success or error.
        /// </returns>
        public List<TopicPartitionError> Pause(IEnumerable<TopicPartition> partitions)
            => kafkaHandle.Pause(partitions);


        /// <summary>
        ///     Resume consumption for the provided list of partitions.
        /// </summary>
        /// <param name="partitions">
        ///     The partitions to resume consumption of.
        /// </param>
        /// <returns>
        ///     Per partition success or error.
        /// </returns>
        public List<TopicPartitionError> Resume(IEnumerable<TopicPartition> partitions)
            => kafkaHandle.Resume(partitions);


        /// <summary>
        ///     Retrieve current committed offsets for topics + partitions.
        ///
        ///     The offset field of each requested partition will be set to the offset
        ///     of the last consumed message, or Offset.Invalid in case there was
        ///     no previous message, or, alternately a partition specific error may also be
        ///     returned.
        ///
        ///     throws KafkaException if there was a problem retrieving the above information.
        /// </summary>
        public Task<List<TopicPartitionOffsetError>> CommittedAsync(
            IEnumerable<TopicPartition> partitions, TimeSpan timeout,
            CancellationToken cancellationToken = default(CancellationToken)
        )
            => Task.Run(() => kafkaHandle.Committed(partitions, (IntPtr)timeout.TotalMillisecondsAsInt()));


        /// <summary>
        ///     Gets the current positions (offsets) for the specified topics + partitions.
        ///
        ///     The offset field of each requested partition will be set to the offset
        ///     of the last consumed message + 1, or Offset.Invalid in case there was
        ///     no previous message consumed by this consumer, or, alternately a partition
        ///     specific error may also be returned.
        /// </summary>
        public Task<List<TopicPartitionOffsetError>> PositionAsync(IEnumerable<TopicPartition> partitions)
            => Task.Run(() => kafkaHandle.Position(partitions));


        /// <summary>
        ///     Look up the offsets for the given partitions by timestamp. The returned offset for each partition is the
        ///     earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding partition.
        /// </summary>
        /// <remarks>
        ///     This is a blocking call. The consumer does not have to be assigned the partitions.
        ///     If the message format version in a partition is before 0.10.0, i.e. the messages do not have timestamps, null
        ///     will be returned for that partition.
        ///     Note that this method may block for the maximum timeout period if the partition does not exist.
        /// </remarks>
        /// <param name="timestampsToSearch">
        ///     The mapping from partition to the timestamp to look up.
        /// </param>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used to cancel this operation.
        /// </param>
        /// <returns>
        ///    A mapping from partition to the timestamp and offset of the first message with timestamp greater
        ///    than or equal to the target timestamp. null will be returned for the partition if there is no such message.
        /// </returns>
        public Task<IEnumerable<TopicPartitionOffsetError>> OffsetsForTimesAsync(
            IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout,
            CancellationToken cancellationToken = default(CancellationToken)
        )
            => Task.Run(() => kafkaHandle.OffsetsForTimes(timestampsToSearch, timeout.TotalMillisecondsAsInt()));


        /// <summary>
        ///     Gets the (dynamic) group member id of this consumer (as set by the broker).
        /// </summary>
        public string MemberId
            => kafkaHandle.MemberId;


        /// <summary>
        ///     Adds one or more brokers to the Client's list of initial
        ///     bootstrap brokers. 
        ///
        ///     Note: Additional brokers are discovered automatically as
        ///     soon as the Client connects to any broker by querying the
        ///     broker metadata. Calling this method is only required in
        ///     some scenarios where the address of all brokers in the
        ///     cluster changes.
        /// </summary>
        /// <param name="brokers">
        ///     Comma-separated list of brokers in the same format as 
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


        /// <summary>
        ///     Gets the name of this client instance.
        ///     Contains (but is not equal to) the client.id configuration parameter.
        /// </summary>
        /// <remarks>
        ///     This name will be unique across all client instances
        ///     in a given application which allows log messages to be
        ///     associated with the corresponding instance.
        /// </remarks>
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
        ///     Alerts the group coodinator that the consumer is exiting the group
        ///     then releases all resources used by this Consumer. If you do not
        ///     call Close before the consumer instance is disposed (or your application
        ///     exits), the group rebalance will not be immediate - it will occur after
        ///     the period of time specified by the broker config `group.max.session.timeout.ms`.
        /// </summary>
        public void Close()
        {
            kafkaHandle.ConsumerClose();
            this.Dispose();
        }


        /// <summary>
        ///     Releases all resources used by this Consumer. Does not block. Typically,
        ///     you should call Close instead of (or in addition to) Dispose to ensure a
        ///     speedy group rebalance. 
        /// </summary>
        public void Dispose()
        {
            // note: consumers always own their own handles.
            KeyDeserializer?.Dispose();
            ValueDeserializer?.Dispose();

            kafkaHandle.Dispose();
        }
    }
}
