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
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Runtime.InteropServices;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Implements a high-level Apache Kafka producer with key
    ///     and value serialization.
    /// </summary>
    public class Producer<TKey, TValue> : IProducer<TKey, TValue>
    {
        private readonly Producer ownedClient;
        private readonly Handle handle;
        private Producer producer;
        private ISerializer<TKey> keySerializer;
        private ISerializer<TValue> valueSerializer;

        private void setAndValidateSerializers(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
        {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;

            if (keySerializer != null && keySerializer == valueSerializer)
            {
                throw new ArgumentException("Key and value serializers must not be the same object.");
            }

            if (keySerializer == null)
            {
                if (typeof(TKey) != typeof(Null))
                {
                    throw new ArgumentNullException("Key serializer must be specified.");
                }
            }

            if (valueSerializer == null)
            {
                if (typeof(TValue) != typeof(Null))
                {
                    throw new ArgumentNullException("Value serializer must be specified.");
                }
            }
        }

        /// <summary>
        ///     Creates a new Producer instance.
        /// </summary>
        /// <param name="config">
        ///     librdkafka configuration parameters (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
        /// </param>
        /// <param name="keySerializer">
        ///     An ISerializer implementation instance that will be used to serialize keys.
        /// </param>
        /// <param name="valueSerializer">
        ///     An ISerializer implementation instance that will be used to serialize values.
        /// </param>
        public Producer(
            IEnumerable<KeyValuePair<string, object>> config,
            ISerializer<TKey> keySerializer,
            ISerializer<TValue> valueSerializer)
        {
            var configWithoutKeySerializerProperties = keySerializer?.Configure(config, true) ?? config;
            var configWithoutValueSerializerProperties = valueSerializer?.Configure(config, false) ?? config;

            var configWithoutSerializerProperties = config.Where(item => 
                configWithoutKeySerializerProperties.Any(ci => ci.Key == item.Key) &&
                configWithoutValueSerializerProperties.Any(ci => ci.Key == item.Key)
            );

            this.ownedClient = new Producer(configWithoutSerializerProperties);
            this.handle = ownedClient.Handle;
            this.producer = ownedClient;
            setAndValidateSerializers(keySerializer, valueSerializer);
        }

        /// <summary>
        ///     Creates a new Producer instance
        /// </summary>
        /// <param name="handle">
        ///     A librdkafka handle to use for Kafka cluster communications.
        /// </param>
        /// <param name="keySerializer">
        ///     An ISerializer implementation instance that will be used to serialize keys.
        /// </param>
        /// <param name="valueSerializer">
        ///     An ISerializer implementation instance that will be used to serialize values.
        /// </param>
        public Producer(
            Handle handle,
            ISerializer<TKey> keySerializer,
            ISerializer<TValue> valueSerializer)
        {
            if (!(handle.Owner is Producer))
            {
                throw new ArgumentException("Handle must be owned by another Producer instance");
            }

            this.ownedClient = null;
            this.handle = handle;
            this.producer = (Producer)handle.Owner;
            setAndValidateSerializers(keySerializer, valueSerializer);
        }

        /// <include file='include_docs_client.xml' path='API/Member[@name="Name"]/*' />
        public string Name
            => this.handle.Owner.Name;

        /// <include file='include_docs_client.xml' path='API/Member[@name="OnLog"]/*' />
        public event EventHandler<LogMessage> OnLog
        {
            add { this.handle.Owner.OnLog += value; }
            remove { this.handle.Owner.OnLog -= value; }
        }

        /// <include file='include_docs_client.xml' path='API/Member[@name="OnStatistics"]/*' />
        public event EventHandler<string> OnStatistics
        {
            add { this.handle.Owner.OnStatistics += value; }
            remove { this.handle.Owner.OnStatistics -= value; }
        }

        /// <include file='include_docs_producer.xml' path='API/Member[@name="OnError"]/*' />
        public event EventHandler<Error> OnError
        {
            add { this.handle.Owner.OnError += value; }
            remove { this.handle.Owner.OnError -= value; }
        }

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Flush_int"]/*' />
        public int Flush(int millisecondsTimeout)
            => ((Producer)this.handle.Owner).Flush(millisecondsTimeout);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Flush_TimeSpan"]/*' />
        public int Flush(TimeSpan timeout)
            => ((Producer)this.handle.Owner).Flush(timeout.TotalMillisecondsAsInt());

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Poll_int"]/*' />
        public int Poll(int millisecondsTimeout)
            => ((Producer)this.handle.Owner).Poll(millisecondsTimeout);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Poll_TimeSpan"]/*' />
        public int Poll(TimeSpan timeout)
            => ((Producer)this.handle.Owner).Poll(timeout.TotalMillisecondsAsInt());

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Dispose"]/*' />
        public void Dispose()
        {
            if (keySerializer != null)
            {
                keySerializer.Dispose();
            }

            if (valueSerializer != null)
            {
                valueSerializer.Dispose();
            }

            if (ownedClient == this.handle.Owner) 
            {
                ownedClient.Dispose();
            }
        }

        /// <include file='include_docs_client.xml' path='API/Member[@name="AddBrokers_string"]/*' />
        public int AddBrokers(string brokers)
            => this.handle.Owner.AddBrokers(brokers);

        /// <summary>
        ///     An opaque reference to the underlying librdkafka client instance.
        /// </summary>
        public Handle Handle 
            => handle;


        private class TypedTaskDeliveryHandlerShim : TaskCompletionSource<DeliveryReport<TKey, TValue>>, IDeliveryHandler
        {
            public TypedTaskDeliveryHandlerShim(string topic, TKey key, TValue val)
#if !NET45
                : base(TaskCreationOptions.RunContinuationsAsynchronously)
#endif
            {
                Topic = topic;
                Key = key;
                Value = val;
            }

            public string Topic;

            public TKey Key;

            public TValue Value;

            public void HandleDeliveryReport(DeliveryReport deliveryReport)
            {
                if (deliveryReport == null)
                {
#if NET45
                    System.Threading.Tasks.Task.Run(() => SetResult(null));
#else
                    SetResult(null);
#endif
                    return;
                }

                var dr = new DeliveryReport<TKey, TValue>
                {
                    TopicPartitionOffsetError = deliveryReport.TopicPartitionOffsetError,
                    Message = new Message<TKey, TValue>
                    {
                        Key = Key,
                        Value = Value,
                        Timestamp = deliveryReport.Message.Timestamp,
                        Headers = deliveryReport.Message.Headers
                    }
                };
                // topic is cached in this object, not set in the deliveryReport to avoid the 
                // cost of marshalling it.
                dr.Topic = Topic;

#if NET45
                if (dr.Error.IsError)
                {
                    System.Threading.Tasks.Task.Run(() => SetException(new ProduceMessageException<TKey, TValue>(dr.Error, dr)));
                }
                else
                {
                    System.Threading.Tasks.Task.Run(() => SetResult(dr));
                }
#else
                if (dr.Error.IsError)
                {
                    SetException(new ProduceMessageException<TKey, TValue>(dr.Error, dr));
                }
                else
                {
                    SetResult(dr);
                }
#endif
            }
        }


        private class TypedDeliveryHandlerShim_Action : IDeliveryHandler
        {
            public TypedDeliveryHandlerShim_Action(string topic, TKey key, TValue val, Action<DeliveryReport<TKey, TValue>> handler)
            {
                Topic = topic;
                Key = key;
                Value = val;
                Handler = handler;
            }

            public string Topic;

            public TKey Key;

            public TValue Value;

            public Action<DeliveryReport<TKey, TValue>> Handler;

            public void HandleDeliveryReport(DeliveryReport deliveryReport)
            {
                if (deliveryReport == null)
                {
                    return;
                }

                var dr = new DeliveryReport<TKey, TValue>
                {
                    TopicPartitionOffsetError = deliveryReport.TopicPartitionOffsetError,
                    Message = new Message<TKey, TValue> 
                    {
                        Key = Key,
                        Value = Value,
                        Timestamp = deliveryReport.Message.Timestamp,
                        Headers = deliveryReport.Message.Headers
                    }
                };
                // topic is cached in this object, not set in the deliveryReport to avoid the 
                // cost of marshalling it.
                dr.Topic = Topic;

                Handler(dr);
            }
        }


        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_Message"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Common"]/*' />
        public Task<DeliveryReport<TKey, TValue>> ProduceAsync(string topic, Message<TKey, TValue> message)
        {
            var handler = new TypedTaskDeliveryHandlerShim(topic,
                producer.enableDeliveryReportKey ? message.Key : default(TKey),
                producer.enableDeliveryReportValue ? message.Value : default(TValue));

            var keyBytes = keySerializer?.Serialize(topic, message.Key);
            var valBytes = valueSerializer?.Serialize(topic, message.Value);

            producer.ProduceImpl(
                topic,
                valBytes, 0, valBytes == null ? 0 : valBytes.Length,
                keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length,
                message.Timestamp, Partition.Any, message.Headers, 
                producer.blockIfQueueFullPropertyValue, handler);

            return handler.Task;
        }

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_TopicPartition_Message"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Common"]/*' />
        public Task<DeliveryReport<TKey, TValue>> ProduceAsync(TopicPartition topicPartition, Message<TKey, TValue> message)
        {
            var handler = new TypedTaskDeliveryHandlerShim(topicPartition.Topic,
                producer.enableDeliveryReportKey ? message.Key : default(TKey),
                producer.enableDeliveryReportValue ? message.Value : default(TValue));

            var keyBytes = keySerializer?.Serialize(topicPartition.Topic, message.Key);
            var valBytes = valueSerializer?.Serialize(topicPartition.Topic, message.Value);
            
            producer.ProduceImpl(
                topicPartition.Topic, 
                valBytes, 0, valBytes == null ? 0 : valBytes.Length, 
                keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length, 
                message.Timestamp, topicPartition.Partition, message.Headers, 
                producer.blockIfQueueFullPropertyValue, handler);

            return handler.Task;
        }

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_TopicPartition_Message"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="Produce_Action"]/*' />
        public void BeginProduce(TopicPartition topicPartition, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler = null)
        {
            var keyBytes = keySerializer?.Serialize(topicPartition.Topic, message.Key);
            var valBytes = valueSerializer?.Serialize(topicPartition.Topic, message.Value);

            producer.ProduceImpl(
                topicPartition.Topic, 
                valBytes, 0, valBytes == null ? 0 : valBytes.Length, 
                keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length, 
                message.Timestamp, topicPartition.Partition, 
                message.Headers, producer.blockIfQueueFullPropertyValue, 
                new TypedDeliveryHandlerShim_Action(
                    topicPartition.Topic,
                    producer.enableDeliveryReportKey ? message.Key : default(TKey),
                    producer.enableDeliveryReportValue ? message.Value : default(TValue),
                    deliveryHandler)
            );
        }

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_Message"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="Produce_Action"]/*' />
        public void BeginProduce(string topic, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler = null)
        {
            var keyBytes = keySerializer?.Serialize(topic, message.Key);
            var valBytes = valueSerializer?.Serialize(topic, message.Value);

            producer.ProduceImpl(
                topic,
                valBytes, 0, valBytes == null ? 0 : valBytes.Length,
                keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length,
                message.Timestamp, Partition.Any,
                message.Headers, producer.blockIfQueueFullPropertyValue,
                new TypedDeliveryHandlerShim_Action(
                    topic,
                    producer.enableDeliveryReportKey ? message.Key : default(TKey), 
                    producer.enableDeliveryReportValue ? message.Value : default(TValue), 
                    deliveryHandler)
            );
        }
    }



    /// <summary>
    ///     Implements a high-level Apache Kafka producer (without serialization).
    /// </summary>
    internal class Producer : IClient
    {
        /// <summary>
        ///     Name of the configuration property that specifies whether or not to
        ///     block if the send queue is full when producing messages. If false, a 
        ///     KafkaExcepion (with Error.Code == ErrorCode.Local_QueueFull) will be 
        ///     thrown if an attempt is made to produce a message and the send queue
        ///     is full.
        ///
        ///     Warning: if this configuration property is set to true, the
        ///     dotnet.producer.manual.poll configuration property is set to true, 
        ///     and Poll is not being called in another thread, this method will 
        ///     block indefinitely in the event it is called when the send queue
        ///     is full.
        /// 
        ///     default: true
        /// </summary>
        public const string BlockIfQueueFullPropertyName = "dotnet.producer.block.if.queue.full";

        /// <summary>
        ///     Name of the configuration property that specifies whether or not 
        ///     the producer should start a background poll thread to receive 
        ///     delivery reports and event notifications. Generally, this should be
        ///     set to true. If set to false, you will need to call the Poll function
        ///     manually.
        /// 
        ///     default: true
        /// </summary>
        public const string EnableBackgroundPollPropertyName = "dotnet.producer.enable.background.poll";

        /// <summary>
        ///     Name of the configuration property that specifies whether to enable 
        ///     notification of delivery reports. If set to false and you use a 
        ///     a ProduceAsync variant, the returned Tasks will never complete and
        ///     will leak memory. Typically you should set this parameter to true. 
        ///     Set it to false for "fire and forget" semantics and a small boost in
        ///     performance.
        /// 
        ///     default: true
        /// </summary>
        public const string EnableDeliveryReportsPropertyName = "dotnet.producer.enable.delivery.reports";

        /// <summary>
        ///     Name of the configuration property that specifies whether to make
        ///     message headers available in delivery reports. Note that 
        ///     disabling header marshaling will improve maximum throughput even 
        ///     for the case where messages do not have any headers.
        /// 
        ///     default: true
        /// </summary>
        public const string EnableDeliveryReportHeadersName = "dotnet.producer.enable.delivery.report.headers";

        /// <summary>
        ///     Name of the configuration property that specifies whether to make
        ///     message keys available in delivery reports. Disabling this will 
        ///     improve maximum throughput and reduce memory usage.
        /// 
        ///     default: true
        /// </summary>
        public const string EnableDeliveryReportKeyName = "dotnet.producer.enable.delivery.report.keys";

        /// <summary>
        ///     Name of the configuration property that specifies whether to make 
        ///     message values available in delivery reports. Disabling this will
        ///     improve maximum throughput and reduce memory usage.
        /// 
        ///     default: true
        /// </summary>
        public const string EnableDeliveryReportValueName = "dotnet.producer.enable.delivery.report.values";

        /// <summary>
        ///     Name of the configuration property that specifies whether to make
        ///     message timestamps available in delivery reports.null Disabling this
        ///     will improve maximum throughput.
        /// 
        ///     default: true
        /// </summary>
        public const string EnableDeliveryReportTimestampName = "dotnet.producer.enable.delivery.report.timestamps";

        private bool manualPoll = false;
        private bool enableDeliveryReports = true;
        internal bool blockIfQueueFullPropertyValue = true;
        internal bool enableDeliveryReportHeaders = true;
        internal bool enableDeliveryReportKey = true;
        internal bool enableDeliveryReportValue = true;
        internal bool enableDeliveryReportTimestamp = true;

        private SafeKafkaHandle kafkaHandle;

        private Task callbackTask;
        private CancellationTokenSource callbackCts;

        private const int POLL_TIMEOUT_MS = 100;
        private Task StartPollTask(CancellationToken ct)
            => Task.Factory.StartNew(() =>
                {
                    try
                    {
                        while (true)
                        {
                            ct.ThrowIfCancellationRequested();
                            kafkaHandle.Poll((IntPtr)POLL_TIMEOUT_MS);
                        }
                    }
                    catch (OperationCanceledException) {}
                }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        private Librdkafka.ErrorDelegate errorDelegate;
        private void ErrorCallback(IntPtr rk, ErrorCode err, string reason, IntPtr opaque)
        {
            OnError?.Invoke(this, new Error(err, reason));
        }

        private Librdkafka.StatsDelegate statsDelegate;
        private int StatsCallback(IntPtr rk, IntPtr json, UIntPtr json_len, IntPtr opaque)
        {
            OnStatistics?.Invoke(this, Util.Marshal.PtrToStringUTF8(json));
            return 0; // instruct librdkafka to immediately free the json ptr.
        }

        private Librdkafka.LogDelegate logDelegate;
        private void LogCallback(IntPtr rk, SyslogLevel level, string fac, string buf)
        {
            var name = Util.Marshal.PtrToStringUTF8(Librdkafka.name(rk));

            if (OnLog == null)
            {
                // Log to stderr by default if no logger is specified.
                Loggers.ConsoleLogger(this, new LogMessage(name, level, fac, buf));
                return;
            }

            OnLog.Invoke(this, new LogMessage(name, level, fac, buf));
        }

        private Librdkafka.DeliveryReportDelegate DeliveryReportCallback;

        /// <remarks>
        ///     note: this property is set to that defined in rd_kafka_conf
        ///     (which is never used by confluent-kafka-dotnet).
        /// </remarks>
        private void DeliveryReportCallbackImpl(IntPtr rk, IntPtr rkmessage, IntPtr opaque)
        {
            var msg = Util.Marshal.PtrToStructureUnsafe<rd_kafka_message>(rkmessage);

            // the msg._private property has dual purpose. Here, it is an opaque pointer set
            // by Topic.Produce to be an IDeliveryHandler. When Consuming, it's for internal
            // use (hence the name).
            if (msg._private == IntPtr.Zero)
            {
                // Note: this can occur if the ProduceAsync overload that accepts a DeliveryHandler
                // was used and the delivery handler was set to null.
                return;
            }

            var gch = GCHandle.FromIntPtr(msg._private);
            var deliveryHandler = (IDeliveryHandler) gch.Target;
            gch.Free();

            Headers headers = null;
            if (this.enableDeliveryReportHeaders) 
            {
                headers = new Headers();
                Librdkafka.message_headers(rkmessage, out IntPtr hdrsPtr);
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
                        headers.Add(headerName, headerValue);
                    }
                }
            }

            IntPtr timestampType = (IntPtr)TimestampType.NotAvailable;
            long timestamp = 0;
            if (enableDeliveryReportTimestamp)
            {
                timestamp = Librdkafka.message_timestamp(rkmessage, out timestampType);
            }

            deliveryHandler.HandleDeliveryReport(
                new DeliveryReport 
                {
                    // Topic is not set here in order to avoid the marshalling cost.
                    // Instead, the delivery handler is expected to cache the topic string.
                    Partition = msg.partition, 
                    Offset = msg.offset, 
                    Error = msg.err,
                    Message = new Message { Timestamp = new Timestamp(timestamp, (TimestampType)timestampType), Headers = headers }
                }
            );
        }

        internal void ProduceImpl(
            string topic,
            byte[] val, int valOffset, int valLength,
            byte[] key, int keyOffset, int keyLength,
            Timestamp timestamp,
            Partition partition, 
            IEnumerable<Header> headers,
            bool blockIfQueueFull,
            IDeliveryHandler deliveryHandler)
        {
            if (timestamp.Type != TimestampType.CreateTime)
            {
                if (timestamp != Timestamp.Default)
                {
                    throw new ArgumentException("Timestamp must be either Timestamp.Default, or timestamp type must equal CreateTime.");
                }
            }

            if (this.enableDeliveryReports && deliveryHandler != null)
            {
                // Passes the TaskCompletionSource to the delivery report callback via the msg_opaque pointer

                // Note: There is a level of indirection between the GCHandle and
                // physical memory address. GCHangle.ToIntPtr doesn't get the
                // physical address, it gets an id that refers to the object via
                // a handle-table.
                var gch = GCHandle.Alloc(deliveryHandler);
                var ptr = GCHandle.ToIntPtr(gch);

                var err = kafkaHandle.Produce(
                    topic,
                    val, valOffset, valLength,
                    key, keyOffset, keyLength,
                    partition.Value,
                    timestamp.UnixTimestampMs,
                    headers,
                    ptr,
                    blockIfQueueFull);
                if (err != ErrorCode.NoError)
                {
                    gch.Free();
                    throw new KafkaException(err);
                }
            }
            else
            {
                var err = kafkaHandle.Produce(
                    topic,
                    val, valOffset, valLength,
                    key, keyOffset, keyLength,
                    partition.Value,
                    timestamp.UnixTimestampMs,
                    headers,
                    IntPtr.Zero, 
                    blockIfQueueFull);

                if (deliveryHandler != null)
                {
                    deliveryHandler.HandleDeliveryReport(null);
                }

                if (err != ErrorCode.NoError)
                {
                    throw new KafkaException(err);
                }
            }
        }


        /// <summary>
        ///     Initializes a new Producer instance.
        /// </summary>
        /// <param name="config">
        ///     librdkafka configuration parameters (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
        /// </param>
        public Producer(IEnumerable<KeyValuePair<string, object>> config)
        {
            // TODO: Make Tasks auto complete when EnableDeliveryReportsPropertyName is set to false.
            // TODO: Hijack the "delivery.report.only.error" configuration parameter and add functionality to enforce that Tasks 
            //       that never complete are never created when this is set to true.

            this.DeliveryReportCallback = DeliveryReportCallbackImpl;

            Librdkafka.Initialize(null);

            var modifiedConfig = config
                .Where(prop => 
                    prop.Key != BlockIfQueueFullPropertyName &&
                    prop.Key != EnableBackgroundPollPropertyName &&
                    prop.Key != EnableDeliveryReportsPropertyName &&
                    prop.Key != EnableDeliveryReportHeadersName &&
                    prop.Key != EnableDeliveryReportKeyName &&
                    prop.Key != EnableDeliveryReportValueName &&
                    prop.Key != EnableDeliveryReportTimestampName);

            var enableBackgroundPollObj = config.FirstOrDefault(prop => prop.Key == EnableBackgroundPollPropertyName).Value;
            if (enableBackgroundPollObj != null)
            {
                this.manualPoll = !bool.Parse(enableBackgroundPollObj.ToString());
            }

            var enableDeliveryReportsObj = config.FirstOrDefault(prop => prop.Key == EnableDeliveryReportsPropertyName).Value;
            if (enableDeliveryReportsObj != null)
            {
                this.enableDeliveryReports = bool.Parse(enableDeliveryReportsObj.ToString());
            }

            var blockIfQueueFullObj = config.FirstOrDefault(prop => prop.Key == BlockIfQueueFullPropertyName).Value;
            if (blockIfQueueFullObj != null)
            {
                this.blockIfQueueFullPropertyValue = bool.Parse(blockIfQueueFullObj.ToString());
            }

            var enableDeliveryReportHeadersObj = config.FirstOrDefault(prop => prop.Key == EnableDeliveryReportHeadersName).Value;
            if (enableDeliveryReportHeadersObj != null)
            {
                this.enableDeliveryReportHeaders = bool.Parse(enableDeliveryReportHeadersObj.ToString());
            }

            var enableDeliveryReportKeyObj = config.FirstOrDefault(prop => prop.Key == EnableDeliveryReportKeyName).Value;
            if (enableDeliveryReportKeyObj != null)
            {
                this.enableDeliveryReportKey = bool.Parse(enableDeliveryReportKeyObj.ToString());
            }

            var enableDeliveryReportValueObj = config.FirstOrDefault(prop => prop.Key == EnableDeliveryReportValueName).Value;
            if (enableDeliveryReportValueObj != null)
            {
                this.enableDeliveryReportValue = bool.Parse(enableDeliveryReportValueObj.ToString());
            }

            var enableDeliveryReportTimestampObj = config.FirstOrDefault(prop => prop.Key == EnableDeliveryReportTimestampName).Value;
            if (enableDeliveryReportTimestampObj != null)
            {
                this.enableDeliveryReportTimestamp = bool.Parse(enableDeliveryReportTimestampObj.ToString());
            }

            // Note: changing the default value of produce.offset.report at the binding level is less than
            // ideal since it means the librdkafka configuration docs will no longer completely match the 
            // .NET client. The default should probably be changed in librdkafka as well.
            if (modifiedConfig.FirstOrDefault(prop => prop.Key == "produce.offset.report").Value == null)
            {
                modifiedConfig = modifiedConfig.Concat(new KeyValuePair<string, object>[] { new KeyValuePair<string, object>("produce.offset.report", "true") });
            }

            var configHandle = SafeConfigHandle.Create();

            modifiedConfig.ToList().ForEach((kvp) => {
                if (kvp.Value == null) throw new ArgumentException($"'{kvp.Key}' configuration parameter must not be null.");
                configHandle.Set(kvp.Key, kvp.Value.ToString());
            });


            IntPtr configPtr = configHandle.DangerousGetHandle();

            if (enableDeliveryReports)
            {
                Librdkafka.conf_set_dr_msg_cb(configPtr, DeliveryReportCallback);
            }

            // Explicitly keep references to delegates so they are not reclaimed by the GC.
            errorDelegate = ErrorCallback;
            logDelegate = LogCallback;
            statsDelegate = StatsCallback;

            // TODO: provide some mechanism whereby calls to the error and log callbacks are cached until
            //       such time as event handlers have had a chance to be registered.
            Librdkafka.conf_set_error_cb(configPtr, errorDelegate);
            Librdkafka.conf_set_log_cb(configPtr, logDelegate);
            Librdkafka.conf_set_stats_cb(configPtr, statsDelegate);

            this.kafkaHandle = SafeKafkaHandle.Create(RdKafkaType.Producer, configPtr);
            configHandle.SetHandleAsInvalid(); // config object is no longer useable.

            if (!manualPoll)
            {
                callbackCts = new CancellationTokenSource();
                callbackTask = StartPollTask(callbackCts.Token);
            }
        }

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Poll_int"]/*' />
        public int Poll(int millisecondsTimeout)
        {
            if (!manualPoll)
            {
                throw new InvalidOperationException("Poll method called when manualPoll not enabled.");
            }
            return this.kafkaHandle.Poll((IntPtr)millisecondsTimeout);
        }

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Poll_TimeSpan"]/*' />
        public int Poll(TimeSpan timeout)
            => Poll(timeout.TotalMillisecondsAsInt());

        /// <include file='include_docs_producer.xml' path='API/Member[@name="OnError"]/*' />
        public event EventHandler<Error> OnError;

        /// <include file='include_docs_client.xml' path='API/Member[@name="OnStatistics"]/*' />
        public event EventHandler<string> OnStatistics;

        /// <include file='include_docs_client.xml' path='API/Member[@name="OnLog"]/*' />
        public event EventHandler<LogMessage> OnLog;

        /// <include file='include_docs_client.xml' path='API/Member[@name="Client_Name"]/*' />
        public string Name
            => kafkaHandle.Name;

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Flush_int"]/*' />
        public int Flush(int millisecondsTimeout)
            => kafkaHandle.Flush(millisecondsTimeout);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Flush_TimeSpan"]/*' />
        public int Flush(TimeSpan timeout)
            => kafkaHandle.Flush(timeout.TotalMillisecondsAsInt());

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Dispose"]/*' />
        public void Dispose()
        {
            // TODO: If this method is called in a finalizer, can callbackTask potentially be null?
            if (!this.manualPoll)
            {
                callbackCts.Cancel();
                try
                {
                    // Note: It's necessary to wait on callbackTask before disposing kafkaHandle
                    // since the poll loop makes use of this.
                    callbackTask.Wait();
                }
                catch (AggregateException e)
                {
                    if (e.InnerException.GetType() != typeof(TaskCanceledException))
                    {
                        throw e.InnerException;
                    }
                }
                finally
                {
                    callbackCts.Dispose();
                }
            }
            kafkaHandle.Dispose();
        }

        /// <include file='include_docs_client.xml' path='API/Member[@name="AddBrokers_string"]/*' />  
        public int AddBrokers(string brokers)
            => kafkaHandle.AddBrokers(brokers);

        /// <summary>
        ///     An opaque reference to the underlying librdkafka client instance.
        /// </summary>
        public Handle Handle 
            => new Handle { Owner = this, LibrdkafkaHandle = kafkaHandle };
    }
}
