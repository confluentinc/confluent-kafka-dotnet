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


namespace Confluent.Kafka
{
    /// <summary>
    ///     A deserializer for use with <see cref="Confluent.Kafka.Consumer{TKey, TValue}" />
    /// </summary>
    /// <param name="topic">
    ///     The topic the data originated from.
    /// </param>
    /// <param name="data">
    ///     The data to deserialize.
    /// </param>
    /// <param name="isNull">
    ///     Whether or not the value is null.
    /// </param>
    /// <returns>
    ///     The deserialized value.
    /// </returns>
    public delegate T Deserializer<T>(string topic, ReadOnlySpan<byte> data, bool isNull);

    /// <summary>
    ///     A generator for <see cref="Confluent.Kafka.Deserializer{T}" /> instances.
    /// </summary>
    /// <param name="forKey">
    ///     Whether or not the deserializer is for use with key data.
    /// </param>
    /// <returns>
    ///     The deserializer.
    /// </returns>
    public delegate Deserializer<T> DeserializerGenerator<T>(bool forKey);

    /// <summary>
    ///     Implements a high-level Apache Kafka consumer.
    /// </summary>
    public class Consumer<TKey, TValue> : IConsumer<TKey, TValue>
    {
        private bool disposeHasBeenCalled = false;
        private object disposeHasBeenCalledLockObj = new object();

        /// <summary>
        ///     keeps track of whether or not assign has been called during
        ///     invocation of a rebalance callback event.
        /// </summary>
        private int assignCallCount = 0;

        private readonly bool enableHeaderMarshaling = true;
        private readonly bool enableTimestampMarshaling = true;
        private readonly bool enableTopicNamesMarshaling = true;

        private Deserializer<TKey> KeyDeserializer { get; }
        private Deserializer<TValue> ValueDeserializer { get; }

        private readonly SafeKafkaHandle kafkaHandle;

        private static readonly byte[] EmptyBytes = new byte[0];


        private readonly Librdkafka.ErrorDelegate errorCallbackDelegate;
        private void ErrorCallback(IntPtr rk, ErrorCode err, string reason, IntPtr opaque)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (kafkaHandle.IsClosed) { return; }
            OnError?.Invoke(this, new ErrorEvent(new Error(err, reason), false));
        }

        private readonly Librdkafka.StatsDelegate statsCallbackDelegate;
        private int StatsCallback(IntPtr rk, IntPtr json, UIntPtr json_len, IntPtr opaque)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (kafkaHandle.IsClosed) { return 0; }
            OnStatistics?.Invoke(this, Util.Marshal.PtrToStringUTF8(json));
            return 0; // instruct librdkafka to immediately free the json ptr.
        }

        private object loggerLockObj = new object();
        private readonly Librdkafka.LogDelegate logCallbackDelegate;
        private void LogCallback(IntPtr rk, SyslogLevel level, string fac, string buf)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            // Note: kafkaHandle can be null if the callback is during construction (in that case the delegate should be called).
            if (kafkaHandle != null && kafkaHandle.IsClosed) { return; }
            OnLog?.Invoke(this, new LogMessage(Util.Marshal.PtrToStringUTF8(Librdkafka.name(rk)), level, fac, buf));
        }

        private readonly Librdkafka.RebalanceDelegate rebalanceDelegate;
        private void RebalanceCallback(
            IntPtr rk,
            ErrorCode err,
            IntPtr partitions,
            IntPtr opaque)
        {
            var partitionList = SafeKafkaHandle.GetTopicPartitionOffsetErrorList(partitions).Select(p => p.TopicPartition).ToList();

            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (kafkaHandle.IsClosed)
            { 
                // The RebalanceCallback should never be invoked as a side effect of Dispose.
                // If for some reason flow of execution gets here, something is badly wrong. 
                // (and we have a closed librdkafka handle that is expecting an assign call...)
                throw new Exception("unexpected rebalance callback on disposed kafkaHandle");
            }

            if (err == ErrorCode.Local_AssignPartitions)
            {
                var handler = OnPartitionsAssigned;
                if (handler != null && handler.GetInvocationList().Length > 0)
                {
                    assignCallCount = 0;
                    handler(this, partitionList);
                    if (assignCallCount == 1) { return; }
                    if (assignCallCount > 1)
                    {
                        throw new InvalidOperationException($"Assign/Unassign was called {assignCallCount} times after OnPartitionsAssigned was raised. It must be called at most once.");
                    }
                }
                Assign(partitionList.Select(p => new TopicPartitionOffset(p, Offset.Invalid)));
            }
            else if (err == ErrorCode.Local_RevokePartitions)
            {
                var handler = OnPartitionsRevoked;
                if (handler != null && handler.GetInvocationList().Length > 0)
                {
                    assignCallCount = 0;
                    handler(this, partitionList);
                    if (assignCallCount == 1) { return; }
                    if (assignCallCount > 1)
                    {
                        throw new InvalidOperationException($"Assign/Unassign was called {assignCallCount} times after OnPartitionsAssigned was raised. It must be called at most once.");
                    }
                }
                Unassign();
            }
        }

        private readonly Librdkafka.CommitDelegate commitDelegate;
        private void CommitCallback(
            IntPtr rk,
            ErrorCode err,
            IntPtr offsets,
            IntPtr opaque)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (kafkaHandle.IsClosed) { return; }

            OnOffsetsCommitted?.Invoke(this, new CommittedOffsets(
                SafeKafkaHandle.GetTopicPartitionOffsetErrorList(offsets),
                new Error(err)
            ));
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IClient.OnLog" />.
        /// </summary>
        public event EventHandler<LogMessage> OnLog;

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IClient.OnError" />.
        /// </summary>
        public event EventHandler<ErrorEvent> OnError;

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IClient.OnStatistics" />.
        /// </summary>
        public event EventHandler<string> OnStatistics;

        /// <summary>
        ///     Creates a new <see cref="Confluent.Kafka.Consumer{TKey, TValue}" /> instance.
        /// </summary>
        /// <param name="config">
        ///     A collection of librdkafka configuration parameters 
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     and parameters specific to this client (refer to: 
        ///     <see cref="Confluent.Kafka.ConfigPropertyNames" />).
        ///     At a minimum, 'bootstrap.servers' and 'group.id' must be
        ///     specified.
        /// </param>
        /// <param name="keyDeserializer">
        ///     A delegate to use for deserializing keys.
        /// </param>
        /// <param name="valueDeserializer">
        ///     A delegate to use for deserializing values.
        /// </param>
        public Consumer(
            IEnumerable<KeyValuePair<string, string>> config,
            Deserializer<TKey> keyDeserializer = null,
            Deserializer<TValue> valueDeserializer = null
        ) : this(config, (isKey) => keyDeserializer, (isKey) => valueDeserializer) {}

        /// <summary>
        ///     Creates a new <see cref="Confluent.Kafka.Consumer{TKey, TValue}" /> instance.
        /// </summary>
        /// <param name="config">
        ///     A collection of librdkafka configuration parameters 
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     and parameters specific to this client (refer to: 
        ///     <see cref="Confluent.Kafka.ConfigPropertyNames" />).
        ///     At a minimum, 'bootstrap.servers' and 'group.id' must be
        ///     specified.
        /// </param>
        /// <param name="keyDeserializerGenerator">
        ///     A delegate to use to create a delegate for deserializing keys.
        /// </param>
        /// <param name="valueDeserializerGenerator">
        ///     A delegate to use to create a delegate for deserializing values.
        /// </param>
        public Consumer(
            IEnumerable<KeyValuePair<string, string>> config,
            DeserializerGenerator<TKey> keyDeserializerGenerator,
            DeserializerGenerator<TValue> valueDeserializerGenerator)
        {
            Librdkafka.Initialize(null);

            KeyDeserializer = keyDeserializerGenerator == null 
                ? null
                : keyDeserializerGenerator(true);
                
            ValueDeserializer = valueDeserializerGenerator == null
                ? null
                : valueDeserializerGenerator(false);

            if (KeyDeserializer != null && (object)KeyDeserializer == (object)ValueDeserializer)
            {
                throw new ArgumentException("Key and value deserializers must not be the same object.");
            }

            if (KeyDeserializer == null)
            {
                if (typeof(TKey) == typeof(Null))
                {
                    KeyDeserializer = Deserializers.Null as Deserializer<TKey>;
                }
                else if (typeof(TKey) == typeof(Ignore))
                {
                    KeyDeserializer = Deserializers.Ignore as Deserializer<TKey>;
                }
                else if (typeof(TKey) == typeof(byte[]))
                {
                    KeyDeserializer = Deserializers.ByteArray as Deserializer<TKey>;
                }
                else if (typeof(TKey) == typeof(string))
                {
                    KeyDeserializer = Deserializers.UTF8 as Deserializer<TKey>;
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
                    ValueDeserializer = Deserializers.Null as Deserializer<TValue>;
                }
                else if (typeof(TValue) == typeof(Ignore))
                {
                    ValueDeserializer = Deserializers.Ignore as Deserializer<TValue>;
                }
                else if (typeof(TValue) == typeof(byte[]))
                {
                    ValueDeserializer = Deserializers.ByteArray as Deserializer<TValue>;
                }
                else if (typeof(TValue) == typeof(string))
                {
                    ValueDeserializer = Deserializers.UTF8 as Deserializer<TValue>;
                }
                else
                {
                    throw new ArgumentNullException("Value deserializer must be specified.");
                }
            }

            if (config.FirstOrDefault(prop => string.Equals(prop.Key, "group.id", StringComparison.Ordinal)).Value == null)
            {
                throw new ArgumentException("'group.id' configuration parameter is required and was not specified.");
            }

            var modifiedConfig = config
                .Where(prop => 
                    prop.Key != ConfigPropertyNames.ConsumerConsumeResultFields);

            var enabledFieldsObj = config.FirstOrDefault(prop => prop.Key == ConfigPropertyNames.ConsumerConsumeResultFields).Value;
            if (enabledFieldsObj != null)
            {
                var fields = enabledFieldsObj.ToString().Replace(" ", "");
                if (fields != "all")
                {
                    this.enableHeaderMarshaling = false;
                    this.enableTimestampMarshaling = false;
                    this.enableTopicNamesMarshaling = false;
                    if (fields != "none")
                    {
                        var parts = fields.Split(',');
                        foreach (var part in parts)
                        {
                            switch (part)
                            {
                                case "headers": this.enableHeaderMarshaling = true; break;
                                case "timestamp": this.enableTimestampMarshaling = true; break;
                                case "topic": this.enableTopicNamesMarshaling = true; break;
                                default: throw new ArgumentException(
                                    $"Unexpected consume result field name '{part}' in config value '{ConfigPropertyNames.ConsumerConsumeResultFields}'.");
                            }
                        }
                    }
                }
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
            errorCallbackDelegate = ErrorCallback;
            logCallbackDelegate = LogCallback;
            statsCallbackDelegate = StatsCallback;

            IntPtr configPtr = configHandle.DangerousGetHandle();

            Librdkafka.conf_set_rebalance_cb(configPtr, rebalanceDelegate);
            Librdkafka.conf_set_offset_commit_cb(configPtr, commitDelegate);

            Librdkafka.conf_set_error_cb(configPtr, errorCallbackDelegate);
            Librdkafka.conf_set_log_cb(configPtr, logCallbackDelegate);
            Librdkafka.conf_set_stats_cb(configPtr, statsCallbackDelegate);

            this.kafkaHandle = SafeKafkaHandle.Create(RdKafkaType.Consumer, configPtr, this);
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


        internal ConsumeResult<TKey, TValue> Consume(int millisecondsTimeout)
        {
            var msgPtr = kafkaHandle.ConsumerPoll((IntPtr)millisecondsTimeout);
            if (msgPtr == IntPtr.Zero)
            {
                return null;
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
                    OnPartitionEOF?.Invoke(this, new TopicPartitionOffset(topic, msg.partition, msg.offset));
                    return null;
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
                            headers.Add(new Header(headerName, headerValue));
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
                            }
                        },
                        new Error(msg.err)
                    );
                }

                TKey key;
                try
                {
                    unsafe
                    {
                        key = this.KeyDeserializer(
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
                            TopicPartitionOffset = new TopicPartitionOffset(topic, msg.partition, msg.offset),
                            Message = new Message<byte[], byte[]>
                            {
                                Timestamp = timestamp,
                                Headers = headers,
                                Key = KeyAsByteArray(msg),
                                Value = ValueAsByteArray(msg)
                            }
                        },
                        new Error(ErrorCode.Local_KeyDeserialization, ex.ToString())
                    );
                }

                TValue val;
                try
                {
                    unsafe
                    {
                        val = this.ValueDeserializer(
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
                            TopicPartitionOffset = new TopicPartitionOffset(topic, msg.partition, msg.offset),
                            Message = new Message<byte[], byte[]>
                            {
                                Timestamp = timestamp,
                                Headers = headers,
                                Key = KeyAsByteArray(msg),
                                Value = ValueAsByteArray(msg)
                            }
                        },
                        new Error(ErrorCode.Local_ValueDeserialization, ex.ToString())
                    );
                }

                return new ConsumeResult<TKey, TValue> 
                { 
                    TopicPartitionOffset = new TopicPartitionOffset(topic, msg.partition, msg.offset),
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
        ///     OnPartitionsAssigned/Revoked, OnOffsetsCommitted and 
        ///     OnPartitionEOF events may be invoked as a side-effect of 
        ///     calling this method (on the same thread).
        /// </remarks>
        public ConsumeResult<TKey, TValue> Consume(TimeSpan timeout)
            => Consume(timeout.TotalMillisecondsAsInt());



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
        ///     OnPartitionsAssigned/Revoked, OnOffsetsCommitted and
        ///     OnPartitionEOF events may be invoked as a side-effect of
        ///     calling this method (on the same thread).
        /// </remarks>
        public ConsumeResult<TKey, TValue> Consume(CancellationToken cancellationToken = default(CancellationToken))
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var result = Consume(100);
                if (result == null) { continue; }
                return result;
            }
        }


        /// <summary>
        ///     Raised when a new partition assignment is received.
        /// 
        ///     If you do not call the <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Assign(IEnumerable{TopicPartition})" />
        ///     method (or another overload of this method) in a handler added to this event, following execution of your handler(s),
        ///     the consumer will be automatically assigned to the set of partitions as specified in the event data and 
        ///     consumption will resume from the last committed offset for each partition, or if there is no committed offset, in
        ///     accordance with the `auto.offset.reset` configuration property. This default behavior will not occur if you call Assign
        ///     yourself in a handler added to this event. The set of partitions you assign to is not required to match the
        ///     assignment given to you, but typically will.
        /// </summary>
        /// <remarks>
        ///     Executes as a side-effect of
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Consume(CancellationToken)" />
        ///     (on the same thread).
        /// </remarks>
        public event EventHandler<List<TopicPartition>> OnPartitionsAssigned;


        /// <summary>
        ///     Raised when a partition assignment is revoked.
        /// 
        ///     If you do not call the <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Unassign" /> or 
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Assign(IEnumerable{TopicPartition})" />
        ///     (or other overload) method in a handler added to this event, all partitions will be 
        ///     automatically unassigned following execution of your handler(s). This default behavior 
        ///     will not occur if you call Unassign (or Assign) yourself in a handler added to this event.
        /// </summary>
        /// <remarks>
        ///     Executes as a side-effect of
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Consume(CancellationToken)" />
        ///     and <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Close()" />
        ///     (on the same thread).
        /// </remarks>
        public event EventHandler<List<TopicPartition>> OnPartitionsRevoked;


        /// <summary>
        ///     Raised to report the result of (automatic) offset commits.
        ///     Not raised as a result of the use of the Commit method.
        /// </summary>
        /// <remarks>
        ///     Executes as a side-effect of
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Consume(CancellationToken)" />
        ///     (on the same thread).
        /// </remarks>
        public event EventHandler<CommittedOffsets> OnOffsetsCommitted;


        /// <summary>
        ///     Raised when the consumer reaches the end of a topic/partition it is reading from.
        /// </summary>
        /// <remarks>
        ///     Executes on the same thread as every other Consumer event handler.
        /// </remarks>
        public event EventHandler<TopicPartitionOffset> OnPartitionEOF;


        /// <summary>
        ///     Gets the current partition assignment as set by
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Assign(TopicPartition)" />.
        /// </summary>
        public List<TopicPartition> Assignment
            => kafkaHandle.GetAssignment();


        /// <summary>
        ///     Gets the current partition subscription as set by
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Subscribe(string)" />.
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
        /// <param name="topics">
        ///     The topics to subscribe to. A regex can be specified to subscribe to 
        ///     the set of all matching topics (which is updated as topics are added
        ///     / removed). A regex must be front anchored to be recognized as a regex.
        ///     e.g. ^myregex
        /// </param>
        public void Subscribe(IEnumerable<string> topics)
            => kafkaHandle.Subscribe(topics);


        /// <summary>
        ///     Update the subscription set to a single topic.
        ///
        ///     Any previous subscription will be unassigned and unsubscribed first.
        /// </summary>
        /// <param name="topic">
        ///     The topic to subscribe to. A regex can be specified to subscribe to 
        ///     the set of all matching topics (which is updated as topics are added
        ///     / removed). A regex must be front anchored to be recognized as a regex.
        ///     e.g. ^myregex
        /// </param>
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
            => Assign(new List<TopicPartition> { partition });


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
            => Assign(new List<TopicPartitionOffset> { partition });


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
        {
            assignCallCount += 1;
            kafkaHandle.Assign(partitions.ToList());
        }


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
        {
            assignCallCount += 1;
            kafkaHandle.Assign(partitions.Select(p => new TopicPartitionOffset(p, Offset.Invalid)).ToList());
        }


        /// <summary>
        ///     Stop consumption and remove the current assignment.
        /// </summary>
        public void Unassign()
        {
            assignCallCount += 1;
            kafkaHandle.Assign(null);
        }

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
        public List<TopicPartitionOffset> Commit(CancellationToken cancellationToken = default(CancellationToken))
            // TODO: use a librdkafka queue for this.
            => kafkaHandle.Commit(null);


        /// <summary>
        ///     Commits an offset based on the topic/partition/offset of a ConsumeResult.
        ///     The next message to be read will be that following <paramref name="result" />.
        /// </summary>
        /// <param name="result">
        ///     The ConsumeResult instance used to determine the committed offset.
        /// </param>
        /// <remarks>
        ///     A consumer which has position N has consumed messages with offsets up to N-1 
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
        public TopicPartitionOffset Commit(
            ConsumeResult<TKey, TValue> result, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (result.Message == null)
            {
                throw new InvalidOperationException("Attempt was made to commit offset corresponding to an empty consume result");
            }

            return kafkaHandle.Commit(new [] { new TopicPartitionOffset(result.TopicPartition, result.Offset + 1) })[0];
        }


        /// <summary>
        ///     Commit an explicit list of offsets.
        /// </summary>
        /// <remarks>
        ///     Note: A consumer which has position N has consumed messages with offsets up to N-1 
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
        public void Commit(IEnumerable<TopicPartitionOffset> offsets, CancellationToken cancellationToken = default(CancellationToken))
            // TODO: use a librdkafka queue for this.
            => kafkaHandle.Commit(offsets);


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
        public List<TopicPartitionOffset> Committed(
            IEnumerable<TopicPartition> partitions, TimeSpan timeout,
            CancellationToken cancellationToken = default(CancellationToken)
        )
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
        public List<TopicPartitionOffset> OffsetsForTimes(
            IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout,
            CancellationToken cancellationToken = default(CancellationToken)
        )
            // TODO: use a librdkafka queue for this.
            => kafkaHandle.OffsetsForTimes(timestampsToSearch, timeout.TotalMillisecondsAsInt());


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
        ///     You should call <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Close" />
        ///     instead of <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Dispose()" />
        ///     (or just before) to ensure a timely consumer-group rebalance. If you
        ///     do not call <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Close" />
        ///     or <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Unsubscribe" />,
        ///     the group will rebalance after a timeout specified by the group's 
        ///     `session.timeout.ms`. Note: the
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.OnPartitionsRevoked" />
        ///     and 
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.OnOffsetsCommitted" />
        ///     events may be called as a side-effect of calling this method.
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
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Close" /> or
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Unsubscribe" />
        ///     prior to Dispose, the group will rebalance after a timeout 
        ///     specified by group's `session.timeout.ms`.
        ///     You should commit offsets / unsubscribe from the group before 
        ///     calling this method (typically by calling 
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Close()" />).
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        /// <summary>
        ///     Releases the unmanaged resources used by the
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}" />
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

    }
}
