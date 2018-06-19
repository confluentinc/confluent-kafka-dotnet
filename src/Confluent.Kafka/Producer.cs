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
    ///     Implements a high-level Apache Kafka producer (without serialization).
    /// </summary>
    public class Producer : IProducer
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
        public string BlockIfQueueFullPropertyName = "dotnet.producer.block.if.queue.full";

        /// <summary>
        ///     Name of the configuration property that specifies whether or not 
        ///     the producer should start a background poll thread to receive 
        ///     delivery reports and event notifications. Generally, this should be
        ///     set to true. If set to false, you will need to call the Poll function
        ///     manually.
        /// 
        ///     default: true
        /// </summary>
        public string EnableBackgroundPollPropertyName = "dotnet.producer.enable.background.poll";

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
        public string EnableDeliveryReportsPropertyName = "dotnet.producer.enable.delivery.reports";

        private bool manualPoll = false;
        private bool disableDeliveryReports = false;
        internal bool blockIfQueueFullPropertyValue = true;

        private bool manualPoll = false;
        private bool disableDeliveryReports = false;
        internal bool blockIfQueueFullPropertyValue = true;
        internal bool enableDeliveryReportHeaderMarshaling = true;
        internal bool enableDeliveryReportDataMarshaling = true;

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
                // Log to stderr by default if no logger is specified.
                Loggers.ConsoleLogger(this, new LogMessage(name, level, fac, buf));
                return;
            }

            OnLog.Invoke(this, new LogMessage(name, level, fac, buf));
        }

        private static readonly LibRdKafka.DeliveryReportDelegate DeliveryReportCallback = DeliveryReportCallbackImpl;

        /// <remarks>
        ///     note: this property is set to that defined in rd_kafka_conf
        ///     (which is never used by confluent-kafka-dotnet).
        /// </remarks>
        private static void DeliveryReportCallbackImpl(IntPtr rk, IntPtr rkmessage, IntPtr opaque)
        {
            var msg = Util.Marshal.PtrToStructureUnsafe<rd_kafka_message>(rkmessage);

            Headers headers = new Headers();
            LibRdKafka.message_headers(rkmessage, out IntPtr hdrsPtr);
            if (hdrsPtr != IntPtr.Zero)
            {
                for (var i=0; ; ++i)
                {
                    var err = LibRdKafka.header_get_all(hdrsPtr, (IntPtr)i, out IntPtr namep, out IntPtr valuep, out IntPtr sizep);
                    if (err != ErrorCode.NoError)
                    {
                        break;
                    }
                    var headerName = Util.Marshal.PtrToStringUTF8(namep);
                    var headerValue = new byte[(int)sizep];
                    Marshal.Copy(valuep, headerValue, 0, (int)sizep);
                    headers.Add(new KeyValuePair<string, byte[]>(headerName, headerValue));
                }
            }

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
            if (deliveryHandler.MarshalHeaders) 
            {
                headers = new Headers();
                LibRdKafka.message_headers(rkmessage, out IntPtr hdrsPtr);
                if (hdrsPtr != IntPtr.Zero)
                {
                    for (var i=0; ; ++i)
                    {
                        var err = LibRdKafka.header_get_all(hdrsPtr, (IntPtr)i, out IntPtr namep, out IntPtr valuep, out IntPtr sizep);
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

            byte[] key = null;
            byte[] val = null;
            if (deliveryHandler.MarshalData)
            {
                if (msg.key != IntPtr.Zero)
                {
                    key = new byte[(int)msg.key_len];
                    System.Runtime.InteropServices.Marshal.Copy(msg.key, key, 0, (int)msg.key_len);
                }
                if (msg.val != IntPtr.Zero)
                {
                    val = new byte[(int)msg.len];
                    System.Runtime.InteropServices.Marshal.Copy(msg.val, val, 0, (int)msg.len);
                }
            }

            IntPtr timestampType;
            long timestamp = LibRdKafka.message_timestamp(rkmessage, out timestampType);

            deliveryHandler.HandleDeliveryReport(
                new DeliveryReport 
                {
                    // TODO: tracking handle -> topicName in addition to topicName -> handle could
                    //       avoid this marshalling / memory allocation cost.
                    Util.Marshal.PtrToStringUTF8(LibRdKafka.topic_name(msg.rkt)),
                    msg.partition,
                    msg.offset,
                    key,
                    val,
                    new Timestamp(timestamp, (TimestampType)timestampType),
                    headers,
                    msg.err
                )
            );
        }

        private sealed class TaskDeliveryHandler : TaskCompletionSource<DeliveryReport>, IDeliveryHandler
        {
#if !NET45
            public TaskDeliveryHandler(bool marshalData, bool marshalHeaders) : base(TaskCreationOptions.RunContinuationsAsynchronously)
            { 
                MarshalData = marshalData;
                MarshalHeaders = marshalHeaders;
            }
#else 
            public TaskDeliveryHandler(bool marshalData, bool marshalHeaders)
            {
                MarshalData = marshalData;
                MarshalHeaders = marshalHeaders;
            }
#endif
            public bool MarshalData { get; private set; }

            public bool MarshalHeaders { get; private set; }

            public void HandleDeliveryReport(DeliveryReport deliveryReport)
            {
#if NET45
                System.Threading.Tasks.Task.Run(() => SetResult(deliveryReport));
#else
                SetResult(deliveryReport);
#endif
            }
        }

        internal void ProduceImpl(
            string topic,
            byte[] val, int valOffset, int valLength,
            byte[] key, int keyOffset, int keyLength,
            Timestamp timestamp,
            Partition partition, 
            IEnumerable<KeyValuePair<string, byte[]>> headers,
            bool blockIfQueueFull,
            IDeliveryHandler deliveryHandler)
        {
            if (timestamp.Type != TimestampType.CreateTime)
            {
                throw new ArgumentException("Timestamp type must be CreateTime");
            }

            if (!this.disableDeliveryReports && deliveryHandler != null)
            {
                // Passes the TaskCompletionSource to the delivery report callback via the msg_opaque pointer
                var deliveryCompletionSource = deliveryHandler;
                var gch = GCHandle.Alloc(deliveryCompletionSource);
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
                if (err != ErrorCode.NoError)
                {
                    throw new KafkaException(err);
                }
            }
        }

        private Task<Message> ProduceImpl(
            string topic,
            byte[] val, int valOffset, int valLength,
            byte[] key, int keyOffset, int keyLength,
            Timestamp timestamp,
            Partition partition, 
            IEnumerable<KeyValuePair<string, byte[]>> headers,
            bool blockIfQueueFull)
        {
            var deliveryCompletionSource = new TaskDeliveryHandler();
            ProduceImpl(topic, val, valOffset, valLength, key, keyOffset, keyLength, timestamp, partition, headers, blockIfQueueFull, deliveryCompletionSource);
            return deliveryCompletionSource.Task;
        }

        private class DeliveryHandlerShim_Action : IDeliveryHandler
        {
            public DeliveryHandlerShim_Action(bool marshalData, bool marshalHeaders, Action<DeliveryReport> handler)
            {
                Handler = handler;
                MarshalData = marshalData;
                MarshalHeaders = marshalHeaders;
            }

            public bool MarshalData { get; private set; }

            public bool MarshalHeaders { get; private set; }

            public Action<DeliveryReport> Handler;

            public void HandleDeliveryReport(DeliveryReport deliveryReport)
            {
                Handler(deliveryReport);
            }
        }

        internal void ProduceImpl(
            string topic,
            byte[] val, int valOffset, int valLength,
            byte[] key, int keyOffset, int keyLength,
            Timestamp timestamp,
            Partition partition, 
            IEnumerable<Header> headers,
            bool blockIfQueueFull,
            Action<DeliveryReport> deliveryHandler
        )
            => ProduceImpl(
                topic, 
                val, valOffset, valLength, 
                key, keyOffset, keyLength, 
                timestamp, 
                partition, 
                headers, 
                blockIfQueueFull, 
                new DeliveryHandlerShim_Action(this.enableDeliveryReportDataMarshaling, this.enableDeliveryReportHeaderMarshaling, deliveryHandler));

        /// <summary>
        ///     Initializes a new Producer instance.
        /// </summary>
        /// <param name="config">
        ///     librdkafka configuration parameters (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
        /// </param>
        /// <param name="manualPoll">
        ///     If true, does not start a dedicated polling thread to trigger events or receive delivery reports -
        ///     you must call the Poll method periodically instead. Typically you should set this parameter to false.
        /// </param>
        /// <param name="disableDeliveryReports">
        ///     If true, disables delivery report notification. Note: if set to true and you use a ProduceAsync variant that returns
        ///     a Task, the Tasks will never complete. Typically you should set this parameter to false. Set it to true for "fire and
        ///     forget" semantics and a small boost in performance.
        /// </param>
        [Obsolete("Use dotnet.producer.enable.background.poll and dotnet.producer.enable.delivery.reports configuration properties " +
                  "instead of manualPoll and disableDeliveryReports constructor parameters.")]
        public Producer(IEnumerable<KeyValuePair<string, object>> config, bool manualPoll, bool disableDeliveryReports)
        {
            // TODO: Make Tasks auto complete when dotnet.producer.enable.delivery.reports is set to false.
            // TODO: Hijack the "delivery.report.only.error" configuration parameter and add functionality to enforce that Tasks 
            //       that never complete are never created when this is set to true.

            LibRdKafka.Initialize(null);

            var modifiedConfig = config
                .Where(
                    prop => prop.Key != "default.topic.config" && 
                    prop.Key != BlockIfQueueFullPropertyName &&
                    prop.Key != EnableBackgroundPollPropertyName &&
                    prop.Key != EnableDeliveryReportsPropertyName &&
                    prop.Key != EnableDeliveryReportHeaderMarshalingName &&
                    prop.Key != EnableDeliveryReportDataMarshalingName);

            var enableBackgroundPollObj = config.FirstOrDefault(prop => prop.Key == EnableBackgroundPollPropertyName).Value;
            if (enableBackgroundPollObj != null)
            {
                this.manualPoll = !bool.Parse(enableBackgroundPollObj.ToString());
            }

            var modifiedConfig = config
                .Where(
                    prop => prop.Key != "default.topic.config" && 
                    prop.Key != BlockIfQueueFullPropertyName &&
                    prop.Key != EnableBackgroundPollPropertyName &&
                    prop.Key != EnableDeliveryReportsPropertyName);

            var enableBackgroundPollObj = config.FirstOrDefault(prop => prop.Key == EnableBackgroundPollPropertyName).Value;
            if (enableBackgroundPollObj != null)
            {
                this.manualPoll = !bool.Parse(enableBackgroundPollObj.ToString());
            }

            var enableDeliveryReportsObj = config.FirstOrDefault(prop => prop.Key == EnableDeliveryReportsPropertyName).Value;
            if (enableDeliveryReportsObj != null)
            {
                this.disableDeliveryReports = !bool.Parse(enableDeliveryReportsObj.ToString());
            }

            var blockIfQueueFullObj = config.FirstOrDefault(prop => prop.Key == BlockIfQueueFullPropertyName).Value;
            if (blockIfQueueFullObj != null)
            {
                this.blockIfQueueFullPropertyValue = bool.Parse(blockIfQueueFullObj.ToString());
            }

            // Note: Setting default topic configuration properties via default.topic.config is depreciated 
            // and this functionality will be removed in a future version of the library.
            var defaultTopicConfig = config.FirstOrDefault(prop => prop.Key == "default.topic.config").Value;
            if (defaultTopicConfig != null)
            {
                modifiedConfig = modifiedConfig.Concat((IEnumerable<KeyValuePair<string, object>>)defaultTopicConfig);
            }

            // Note: changing the default value of produce.offset.report at the binding level is less than
            // ideal since it means the librdkafka configuration docs will no longer completely match the 
            // .NET client. The default should probably be changed in librdkafka as well.
            if (modifiedConfig.FirstOrDefault(prop => prop.Key == "produce.offset.report").Value == null)
            {
                modifiedConfig = modifiedConfig.Concat(new KeyValuePair<string, object>[] { new KeyValuePair<string, object>("produce.offset.report", "true") });
            }

            var configHandle = SafeConfigHandle.Create();

            modifiedConfig.ToList().ForEach((kvp) => { configHandle.Set(kvp.Key, kvp.Value.ToString()); });

            IntPtr configPtr = configHandle.DangerousGetHandle();

            if (!disableDeliveryReports)
            {
                LibRdKafka.conf_set_dr_msg_cb(configPtr, DeliveryReportCallback);
            }

            // Explicitly keep references to delegates so they are not reclaimed by the GC.
            errorDelegate = ErrorCallback;
            logDelegate = LogCallback;
            statsDelegate = StatsCallback;

            // TODO: provide some mechanism whereby calls to the error and log callbacks are cached until
            //       such time as event handlers have had a chance to be registered.
            LibRdKafka.conf_set_error_cb(configPtr, errorDelegate);
            LibRdKafka.conf_set_log_cb(configPtr, logDelegate);
            LibRdKafka.conf_set_stats_cb(configPtr, statsDelegate);

            this.kafkaHandle = SafeKafkaHandle.Create(RdKafkaType.Producer, configPtr);
            configHandle.SetHandleAsInvalid(); // config object is no longer useable.

            if (!manualPoll)
            {
                callbackCts = new CancellationTokenSource();
                callbackTask = StartPollTask(callbackCts.Token);
            }
        }

        /// <summary>
        ///     Initializes a new Producer instance.
        /// </summary>
        /// <param name="config">
        ///     librdkafka configuration parameters (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
        /// </param>
        public Producer(IEnumerable<KeyValuePair<string, object>> config)
#pragma warning disable CS0618
            : this(config, false, false) {}
#pragma warning restore CS0618

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

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Poll"]/*' />
        [Obsolete("Use an overload of Poll with a finite timeout.", false)]
        public int Poll()
            => Poll(-1);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="OnError"]/*' />
        public event EventHandler<Error> OnError;

        /// <include file='include_docs_client.xml' path='API/Member[@name="OnStatistics"]/*' />
        public event EventHandler<string> OnStatistics;

        /// <include file='include_docs_client.xml' path='API/Member[@name="OnLog"]/*' />
        public event EventHandler<LogMessage> OnLog;

        /// <include file='include_docs_producer.xml' path='API/Member[@name="GetSerializingProducer"]/*' />
        public ISerializingProducer<TKey, TValue> GetSerializingProducer<TKey, TValue>(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
            => new SerializingProducer<TKey, TValue>(this, keySerializer, valueSerializer);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Message"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Common"]/*' />
        public Task<Message> ProduceAsync(Message message)
            => ProduceImpl(
                message.Topic, 
                message.Value, 0, message.Value?.Length ?? 0, 
                message.Key, 0, message.Key?.Length ?? 0, 
                message.Timestamp.Type == TimestampType.CreateTime ? message.Timestamp : Timestamp.Default,
                message.Partition, 
                message.Headers, 
                this.blockIfQueueFullPropertyValue);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_TKey_TValue"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Common"]/*' />
        public Task<Message> ProduceAsync(string topic, byte[] key, byte[] val)
            => ProduceImpl(topic, val, 0, val?.Length ?? 0, key, 0, key?.Length ?? 0, Timestamp.Default, Partition.Any, null, this.blockIfQueueFullPropertyValue);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_Partition_TKey_TValue_Timestamp_IEnumerable"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Common"]/*' />
        public Task<Message> ProduceAsync(
            string topic, Partition partition, 
            byte[] key, byte[] val, 
            Timestamp timestamp, IEnumerable<KeyValuePair<string, byte[]>> headers
        )
            => ProduceImpl(topic, val, 0, val?.Length ?? 0, key, 0, key?.Length ?? 0, timestamp, partition, headers, this.blockIfQueueFullPropertyValue);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_Partition_byte_int_int_byte_int_int_Timestamp_IEnumerable"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Common"]/*' />
        public Task<Message> ProduceAsync(
            string topic, Partition partition, 
            byte[] key, int keyOffset, int keyLength, 
            byte[] val, int valOffset, int valLength, 
            Timestamp timestamp, 
            IEnumerable<KeyValuePair<string, byte[]>> headers
        )
            => ProduceImpl(topic, val, valOffset, valLength, key, keyOffset, keyLength, timestamp, partition, headers, this.blockIfQueueFullPropertyValue);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Message"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="Produce_IDeliveryHandler"]/*' />
        public void Produce(Message message, IDeliveryHandler deliveryHandler)
            => ProduceImpl(
                message.Topic, 
                message.Value, 0, message.Value?.Length ?? 0, 
                message.Key, 0, message.Key?.Length ?? 0, 
                message.Timestamp.Type == TimestampType.CreateTime ? message.Timestamp : Timestamp.Default,
                message.Partition, 
                message.Headers, 
                this.blockIfQueueFullPropertyValue, 
                deliveryHandler);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_TKey_TValue"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="Produce_IDeliveryHandler"]/*' />
        public void Produce(string topic, byte[] key, byte[] val, IDeliveryHandler deliveryHandler)
            => ProduceImpl(topic, val, 0, val?.Length ?? 0, key, 0, key?.Length ?? 0, Timestamp.Default, Partition.Any, null, this.blockIfQueueFullPropertyValue, deliveryHandler);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_Partition_TKey_TValue_Timestamp_IEnumerable"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="Produce_IDeliveryHandler"]/*' />
        public void Produce(
            string topic, Partition partition, 
            byte[] key, byte[] val, 
            Timestamp timestamp, IEnumerable<KeyValuePair<string, byte[]>> headers, 
            IDeliveryHandler deliveryHandler
        )
            => ProduceImpl(topic, val, 0, val?.Length ?? 0, key, 0, key?.Length ?? 0, timestamp, partition, headers, this.blockIfQueueFullPropertyValue, deliveryHandler);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_Partition_byte_int_int_byte_int_int_Timestamp_IEnumerable"]/*' />        
        /// <include file='include_docs_producer.xml' path='API/Member[@name="Produce_IDeliveryHandler"]/*' />
        public void Produce(
            string topic, Partition partition, 
            byte[] key, int keyOffset, int keyLength, 
            byte[] val, int valOffset, int valLength, 
            Timestamp timestamp, IEnumerable<KeyValuePair<string, byte[]>> headers, 
            IDeliveryHandler deliveryHandler
        )
            => ProduceImpl(topic, val, valOffset, valLength, key, keyOffset, keyLength, timestamp, partition, headers, this.blockIfQueueFullPropertyValue, deliveryHandler);

#region obsolete produce methods

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete("The Producer API has been revised and this overload of ProduceAsync has been depreciated. Please use another variant of ProduceAsync.")]
        public Task<Message> ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength)
            => ProduceImpl(topic, val, valOffset, valLength, key, keyOffset, keyLength, Timestamp.Default, Partition.Any, null, this.blockIfQueueFullPropertyValue);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete("The Producer API has been revised and this overload of ProduceAsync has been depreciated. Please use another variant of ProduceAsync.")]
        public Task<Message> ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, int partition)
            => ProduceImpl(topic, val, valOffset, valLength, key, keyOffset, keyLength, Timestamp.Default, partition, null, this.blockIfQueueFullPropertyValue);
        
        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete("Variants of ProduceAsync that include a blockIfQueueFull parameter are depreciated - use the dotnet.producer.block.if.queue.full configuration property instead.")]
        public Task<Message> ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, int partition, bool blockIfQueueFull)
            => ProduceImpl(topic, val, valOffset, valLength, key, keyOffset, keyLength, Timestamp.Default, partition, null, blockIfQueueFull);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete("Variants of ProduceAsync that include a blockIfQueueFull parameter are depreciated - use the dotnet.producer.block.if.queue.full configuration property instead.")]
        public Task<Message> ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, bool blockIfQueueFull)
            => ProduceImpl(topic, val, valOffset, valLength, key, keyOffset, keyLength, Timestamp.Default, Partition.Any, null, blockIfQueueFull);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete("Variants of ProduceAsync that include a IDeliveryHandler parameter are depreciated - use a variant of Produce instead.")]
        public void ProduceAsync(string topic, byte[] key, byte[] val, IDeliveryHandler deliveryHandler)
            => ProduceImpl(topic, val, 0, val?.Length ?? 0, key, 0, key?.Length ?? 0, Timestamp.Default, Partition.Any, null, this.blockIfQueueFullPropertyValue, deliveryHandler);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete("Variants of ProduceAsync that include a IDeliveryHandler parameter are depreciated - use a variant of Produce instead.")]
        public void ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, IDeliveryHandler deliveryHandler)
            => ProduceImpl(topic, val, valOffset, valLength, key, keyOffset, keyLength, Timestamp.Default, Partition.Any, null, this.blockIfQueueFullPropertyValue, deliveryHandler);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete("Variants of ProduceAsync that include a IDeliveryHandler parameter are depreciated - use a variant of Produce instead.")]
        public void ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, int partition, IDeliveryHandler deliveryHandler)
            => ProduceImpl(topic, val, valOffset, valLength, key, keyOffset, keyLength, Timestamp.Default, partition, null, this.blockIfQueueFullPropertyValue, deliveryHandler);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete(
            "Variants of ProduceAsync that include a IDeliveryHandler parameter are depreciated - use a variant of Produce instead. " +
            "Variants of ProduceAsync that include a blockIfQueueFull parameter are depreciated - use the dotnet.producer.block.if.queue.full configuration property instead.")]
        public void ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, int partition, bool blockIfQueueFull, IDeliveryHandler deliveryHandler)
            => ProduceImpl(topic, val, valOffset, valLength, key, keyOffset, keyLength, Timestamp.Default, partition, null, blockIfQueueFull, deliveryHandler);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete(
            "Variants of ProduceAsync that include a IDeliveryHandler parameter are depreciated - use a variant of Produce instead. " +
            "Variants of ProduceAsync that include a blockIfQueueFull parameter are depreciated - use the dotnet.producer.block.if.queue.full configuration property instead.")]
        public void ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, bool blockIfQueueFull, IDeliveryHandler deliveryHandler)
            => ProduceImpl(topic, val, valOffset, valLength, key, keyOffset, keyLength, Timestamp.Default, Partition.Any, null, blockIfQueueFull, deliveryHandler);

#endregion

        /// <include file='include_docs_client.xml' path='API/Member[@name="Client_Name"]/*' />
        public string Name
            => kafkaHandle.Name;

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Flush_int"]/*' />
        public int Flush(int millisecondsTimeout)
            => kafkaHandle.Flush(millisecondsTimeout);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Flush_TimeSpan"]/*' />
        public int Flush(TimeSpan timeout)
            => kafkaHandle.Flush(timeout.TotalMillisecondsAsInt());

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Flush"]/*' />
        [Obsolete("Use an overload of Flush with a finite timeout.", false)]
        public int Flush()
            => kafkaHandle.Flush(-1);

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

        /// <include file='include_docs_client.xml' path='API/Member[@name="ListGroups_TimeSpan"]/*' />
        public List<GroupInfo> ListGroups(TimeSpan timeout)
            => kafkaHandle.ListGroups(timeout.TotalMillisecondsAsInt());

        /// <include file='include_docs_client.xml' path='API/Member[@name="ListGroup_string_TimeSpan"]/*' />
        public GroupInfo ListGroup(string group, TimeSpan timeout)
            => kafkaHandle.ListGroup(group, timeout.TotalMillisecondsAsInt());

        /// <include file='include_docs_client.xml' path='API/Member[@name="ListGroup_string"]/*' />
        public GroupInfo ListGroup(string group)
            => kafkaHandle.ListGroup(group, -1);

        /// <include file='include_docs_client.xml' path='API/Member[@name="QueryWatermarkOffsets_TopicPartition_TimeSpan"]/*' />
        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
            => kafkaHandle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, timeout.TotalMillisecondsAsInt());

        /// <include file='include_docs_client.xml' path='API/Member[@name="QueryWatermarkOffsets_TopicPartition"]/*' />
        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition)
            => kafkaHandle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, -1);

        private Metadata GetMetadata(bool allTopics, string topic, int millisecondsTimeout)
            => kafkaHandle.GetMetadata(allTopics, topic == null ? null : getKafkaTopicHandle(topic), millisecondsTimeout);

        /// <include file='include_docs_client.xml' path='API/Member[@name="GetMetadata_bool_string_TimeSpan"]/*' />
        public Metadata GetMetadata(bool allTopics, string topic, TimeSpan timeout)
            => GetMetadata(allTopics, topic, timeout.TotalMillisecondsAsInt());

        /// <include file='include_docs_client.xml' path='API/Member[@name="GetMetadata_bool_string"]/*' />
        public Metadata GetMetadata(bool allTopics, string topic)
            => GetMetadata(allTopics, topic, -1);

        /// <include file='include_docs_client.xml' path='API/Member[@name="GetMetadata"]/*' />
        public Metadata GetMetadata()
            => GetMetadata(true, null, -1);

        /// <include file='include_docs_client.xml' path='API/Member[@name="AddBrokers_string"]/*' />  
        public int AddBrokers(string brokers)
            => kafkaHandle.AddBrokers(brokers);
    }
}
