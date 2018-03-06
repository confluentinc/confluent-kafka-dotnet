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
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Runtime.InteropServices;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Serialization;
using System.Collections.Concurrent;

namespace Confluent.Kafka
{
    /// <summary>
    ///     Implements a high-level Apache Kafka producer (without serialization).
    /// </summary>
    public class Producer : IDisposable
    {
        private bool manualPoll;
        private bool disableDeliveryReports;

        internal const int RD_KAFKA_PARTITION_UA = -1;
        internal const long RD_KAFKA_NO_TIMESTAMP = 0;

        private ConcurrentDictionary<string, SafeTopicHandle> topicHandles
            = new ConcurrentDictionary<string, SafeTopicHandle>(StringComparer.Ordinal);

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
                            this.kafkaHandle.Poll((IntPtr)POLL_TIMEOUT_MS);
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

        private readonly Func<string, SafeTopicHandle> topicHandlerFactory;

        /// <remarks>
        ///     getKafkaTopicHandle() is now only required by GetMetadata() which still requires that 
        ///     topic is specified via a handle rather than a string name (note that getKafkaTopicHandle() 
        ///     was also formerly required by the ProduceAsync methods). Eventually we would like to 
        ///     depreciate this method as well as the SafeTopicHandle class.
        /// </remarks>

        private SafeTopicHandle getKafkaTopicHandle(string topic) 
            => topicHandles.GetOrAdd(topic, topicHandlerFactory);

        private static readonly LibRdKafka.DeliveryReportDelegate DeliveryReportCallback = DeliveryReportCallbackImpl;

        /// <remarks>
        ///     note: this property is set to that defined in rd_kafka_conf
        ///     (which is never used by confluent-kafka-dotnet).
        /// </remarks>
        private static void DeliveryReportCallbackImpl(IntPtr rk, IntPtr rkmessage, IntPtr opaque)
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
                new Message (
                    // TODO: tracking handle -> topicName in addition to topicName -> handle could
                    //       avoid this marshalling / memory allocation cost.
                    Util.Marshal.PtrToStringUTF8(LibRdKafka.topic_name(msg.rkt)),
                    msg.partition,
                    msg.offset,
                    key,
                    val,
                    new Timestamp(timestamp, (TimestampType)timestampType),
                    msg.err
                )
            );
        }

        private sealed class TaskDeliveryHandler : TaskCompletionSource<Message>, IDeliveryHandler
        {
#if !NET45
            public TaskDeliveryHandler() : base(TaskCreationOptions.RunContinuationsAsynchronously)
            { }
#endif
            public bool MarshalData { get { return true; } }

            public void HandleDeliveryReport(Message message)
            {
#if NET45
                System.Threading.Tasks.Task.Run(() => SetResult(message));
#else
                SetResult(message);
#endif
            }
        }

        internal void Produce(
            string topic,
            byte[] val, int valOffset, int valLength,
            byte[] key, int keyOffset, int keyLength,
            DateTime? timestamp,
            Int32 partition, bool blockIfQueueFull,
            IDeliveryHandler deliveryHandler)
        {
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
                    partition,
                    timestamp == null ? RD_KAFKA_NO_TIMESTAMP : Timestamp.DateTimeToUnixTimestampMs(timestamp.Value),
                    ptr, blockIfQueueFull);
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
                    partition,
                    timestamp == null ? RD_KAFKA_NO_TIMESTAMP : Timestamp.DateTimeToUnixTimestampMs(timestamp.Value),
                    IntPtr.Zero, blockIfQueueFull);
                if (err != ErrorCode.NoError)
                {
                    throw new KafkaException(err);
                }
            }
        }

        private Task<Message> Produce(
            string topic,
            byte[] val, int valOffset, int valLength,
            byte[] key, int keyOffset, int keyLength,
            DateTime? timestamp,
            Int32? partition, bool blockIfQueueFull
        )
        {
            var deliveryCompletionSource = new TaskDeliveryHandler();
            Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, timestamp, partition != null ? partition.Value : RD_KAFKA_PARTITION_UA, blockIfQueueFull, deliveryCompletionSource);
            return deliveryCompletionSource.Task;
        }

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
        public Producer(IEnumerable<KeyValuePair<string, object>> config, bool manualPoll, bool disableDeliveryReports)
        {
            LibRdKafka.Initialize(null);

            this.manualPoll = manualPoll;
            this.disableDeliveryReports = disableDeliveryReports;

            var configHandle = SafeConfigHandle.Create();

            var modifiedConfig = config
                .Where(prop => prop.Key != "default.topic.config");

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

            // note: ConcurrentDictionary.GetorAdd() method is not atomic
            this.topicHandlerFactory = (string topicName) =>
            {
                // Note: there is a possible (benign) race condition here - topicHandle could have already
                // been created for the topic (and possibly added to topicHandles). If the topicHandle has
                // already been created, rdkafka will return it and not create another. the call to rdkafka
                // is threadsafe.
                return kafkaHandle.Topic(topicName, IntPtr.Zero);
            };
        }

        /// <summary>
        ///     Initializes a new Producer instance.
        /// </summary>
        /// <param name="config">
        ///     librdkafka configuration parameters (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
        /// </param>
        public Producer(IEnumerable<KeyValuePair<string, object>> config)
            : this(config, false, false) {}


        /// <summary>
        ///     Poll for callback events. You will not typically need
        ///     to call this method. Only call on producer instances 
        ///     where background polling is not enabled.
        /// </summary>
        /// <param name="millisecondsTimeout">
        ///     The maximum period of time to block (in milliseconds) if no
        ///     callback events are waiting or -1 to block indefinitely. 
        ///     You should typically use a relatively short timout period 
        ///     because this operation cannot be cancelled.
        /// </param>
        /// <returns>
        ///     Returns the number of events served.
        /// </returns>
        public int Poll(int millisecondsTimeout)
        {
            if (!manualPoll)
            {
                throw new InvalidOperationException("Poll method called when manualPoll not enabled.");
            }
            return this.kafkaHandle.Poll((IntPtr)millisecondsTimeout);
        }

        /// <summary>
        ///     Poll for callback events. You will not typically need
        ///     to call this method. Only call on producer instances 
        ///     where background polling is not enabled.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum period of time to block if no callback events
        ///     are waiting. You should typically use a relatively short 
        ///     timout period because this operation cannot be cancelled.
        /// </param>
        /// <returns>
        ///     Returns the number of events served.
        /// </returns>
        public int Poll(TimeSpan timeout)
            => Poll(timeout.TotalMillisecondsAsInt());

        /// <summary>
        ///     Poll for callback events. You will not typically need
        ///     to call this method. Only call on producer
        ///     instances where background polling is not enabled.
        ///     Blocks until there is a callback event ready to be served.
        /// </summary>
        /// <returns>
        ///     Returns the number of events served.
        /// </returns>
        [Obsolete("Use an overload of Poll with a finite timeout.", false)]
        public int Poll()
            => Poll(-1);


        /// <summary>
        ///     Raised on critical errors, e.g. connection failures or all 
        ///     brokers down. Note that the client will try to automatically 
        ///     recover from errors - these errors should be seen as 
        ///     informational rather than catastrophic
        /// </summary>
        /// <remarks>
        ///     Called on the Producer poll thread.
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
        ///     Called on the Producer poll thread.
        /// </remarks>
        public event EventHandler<string> OnStatistics;

        /// <summary>
        ///     Raised when there is information that should be logged.
        /// </summary>
        /// <remarks>
        ///     By default not many log messages are generated.
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
        ///     Returns a serializing producer that uses this Producer to 
        ///     produce messages. The same underlying Producer can be used
        ///     as the basis of many serializing producers (potentially with
        ///     different TKey and TValue types). Threadsafe.
        /// </summary>
        /// <param name="keySerializer">
        ///     The key serializer.
        /// </param>
        /// <param name="valueSerializer">
        ///     The value serializer.
        /// </param>
        /// <typeparam name="TKey">
        ///     The key type.
        /// </typeparam>
        /// <typeparam name="TValue">
        ///     The value type.
        /// </typeparam>
        public ISerializingProducer<TKey, TValue> GetSerializingProducer<TKey, TValue>(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
            => new SerializingProducer<TKey, TValue>(this, keySerializer, valueSerializer);


        /// <summary>
        ///     Asynchronously send a single message to the broker.
        ///     Refer to <see cref="ProduceAsync(string, byte[], int, int, byte[], int, int, int, bool)" /> 
        ///     for more information.
        /// </summary>
        public Task<Message> ProduceAsync(string topic, byte[] key, byte[] val)
            => Produce(topic, val, 0, val?.Length ?? 0, key, 0, key?.Length ?? 0, null, RD_KAFKA_PARTITION_UA, true);


        /// <summary>
        ///     Asynchronously send a single message to the broker.
        ///     Refer to <see cref="ProduceAsync(string, byte[], int, int, byte[], int, int, int, bool)" /> 
        ///     for more information.
        /// </summary>
        public Task<Message> ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, null, RD_KAFKA_PARTITION_UA, true);

        /// <summary>
        ///     Asynchronously send a single message to the broker.
        ///     Refer to <see cref="ProduceAsync(string, byte[], int, int, byte[], int, int, int, bool)" /> 
        ///     for more information.
        /// </summary>
        public Task<Message> ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, int partition)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, null, partition, true);

        /// <summary>
        ///     Asynchronously send a single message to the broker.
        /// </summary>
        /// <param name="topic">
        ///     The target topic.
        /// </param>
        /// <param name="partition">
        ///     The target partition (if -1, this is determined by the partitioner
        ///     configured for the topic).
        /// </param>
        /// <param name="key">
        ///     null, or a byte array that contains the message key.
        /// </param>
        /// <param name="keyOffset">
        ///     for non-null values, the offset into the key array of the
        ///     sub-array to use as the message key.
        ///     if <paramref name="key" />  is null, keyOffset must be 0.
        /// </param>
        /// <param name="keyLength">
        ///     for non-null keys, the length of the sequence of bytes that
        ///     constitutes the key.
        ///     if <paramref name="key" />  is null, keyOffset must be 0.
        /// </param>
        /// <param name="val">
        ///     null, or a byte array that contains the message value.
        /// </param>
        /// <param name="valOffset">
        ///     for non-null values, the offset into the val array of the
        ///     sub-array to use as the message value.
        ///     if <paramref name="val" /> is null, valOffset must be 0.
        /// </param>
        /// <param name="valLength">
        ///     for non-null values, the length of the sequence of bytes that
        ///     constitutes the value.
        ///     if <paramref name="val" /> is null, valLength must be 0.
        /// </param>
        /// <param name="blockIfQueueFull">
        ///     Whether or not to block if the send queue is full.
        ///     If false, a KafkaExcepion (with Error.Code == ErrorCode.Local_QueueFull) 
        ///     will be thrown if an attempt is made to produce a message
        ///     and the send queue is full.
        /// 
        ///     Warning: blockIfQueueFull is set to true, background polling is 
        ///     disabled and Poll is not being called in another thread, this
        ///     will block indefinitely.
        /// </param>
        /// <returns>
        ///     A Task which will complete with the corresponding delivery report
        ///     for this request.
        /// </returns>
        /// <remarks>
        ///     If you require strict ordering of delivery reports to be maintained, 
        ///     you should use a variant of ProduceAsync that takes an IDeliveryHandler
        ///     parameter, not a variant that returns a Task&lt;Message&gt; because 
        ///     Tasks are completed on arbitrary thread pool threads and can 
        ///     be executed out of order.
        /// </remarks>
        public Task<Message> ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, int partition, bool blockIfQueueFull)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, null, partition, blockIfQueueFull);

        /// <summary>
        ///     Asynchronously send a single message to the broker.
        ///     Refer to <see cref="ProduceAsync(string, byte[], int, int, byte[], int, int, int, bool)" />
        ///     for more information.
        /// </summary>
        public Task<Message> ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, bool blockIfQueueFull)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, null, RD_KAFKA_PARTITION_UA, blockIfQueueFull);


        /// <summary>
        ///     Asynchronously send a single message to the broker (order of delivery reports strictly guarenteed).
        ///     Refer to <see cref="ProduceAsync(string, byte[], int, int, byte[], int, int, int, bool, IDeliveryHandler)" /> 
        ///     for more information.
        /// </summary>
        public void ProduceAsync(string topic, byte[] key, byte[] val, IDeliveryHandler deliveryHandler)
            => Produce(topic, val, 0, val?.Length ?? 0, key, 0, key?.Length ?? 0, null, RD_KAFKA_PARTITION_UA, true, deliveryHandler);


        /// <summary>
        ///     Asynchronously send a single message to the broker (order of delivery reports strictly guarenteed).
        ///     Refer to <see cref="ProduceAsync(string, byte[], int, int, byte[], int, int, int, bool, IDeliveryHandler)" />
        ///     for more information.
        /// </summary>
        public void ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, IDeliveryHandler deliveryHandler)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, null, RD_KAFKA_PARTITION_UA, true, deliveryHandler);

        /// <summary>
        ///     Asynchronously send a single message to the broker (order of delivery reports strictly guarenteed).
        ///     Refer to <see cref="ProduceAsync(string, byte[], int, int, byte[], int, int, int, bool, IDeliveryHandler)" /> 
        ///     for more information.
        /// </summary>
        public void ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, int partition, IDeliveryHandler deliveryHandler)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, null, partition, true, deliveryHandler);

        /// <summary>
        ///     Asynchronously send a single message to the broker (order of delivery reports strictly guarenteed).
        /// </summary>
        /// <remarks>
        ///     Notification of delivery reports is via an IDeliveryHandler instance. Use IDeliveryHandler variants of 
        ///     ProduceAsync if you require notification of delivery reports strictly in the order they were 
        ///     acknowledged by the broker / failed (failure may be via broker or local). IDeliveryHandler.HandleDeliveryReport
        ///     callbacks are executed on the Poll thread.
        ///     
        ///     Refer to <see cref="ProduceAsync(string, byte[], int, int, byte[], int, int, int, bool)" /> 
        ///     for more information.
        /// </remarks>
        public void ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, int partition, bool blockIfQueueFull, IDeliveryHandler deliveryHandler)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, null, partition, blockIfQueueFull, deliveryHandler);

        /// <summary>
        ///     Asynchronously send a single message to the broker (order of delivery reports strictly guarenteed).
        ///     Refer to <see cref="ProduceAsync(string, byte[], int, int, byte[], int, int, int, bool, IDeliveryHandler)" /> 
        ///     for more information.
        /// </summary>
        public void ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, bool blockIfQueueFull, IDeliveryHandler deliveryHandler)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, null, RD_KAFKA_PARTITION_UA, blockIfQueueFull, deliveryHandler);

        /// <summary>
        ///     Gets the name of this producer instance.
        ///     Contains (but is not equal to) the client.id configuration parameter.
        /// </summary>
        /// <remarks>
        ///     This name will be unique across all producer instances
        ///     in a given application which allows log messages to be
        ///     associated with the corresponding instance.
        /// </remarks>
        public string Name
            => kafkaHandle.Name;

        /// <summary>
        ///     Wait until all outstanding produce requests and delievery report
        ///     callbacks are completed.
        /// 
        ///     [UNSTABLE-API] - the semantics and/or type of the return value is
        ///     subject to change.
        /// </summary>
        /// <param name="millisecondsTimeout">
        ///     The maximum time to block in milliseconds or -1 to block
        ///     indefinitely. You should typically use a relatively short timout 
        ///     period because this operation cannot be cancelled.
        /// </param>
        /// <returns>
        ///     The current librdkafka out queue length. This should be interpreted
        ///     as a rough indication of the number of messages waiting to be sent
        ///     to or acknowledged by the broker. If zero, there are no outstanding
        ///     messages or callbacks. Specifically, the value is equal to the sum
        ///     of the number of produced messages for which a delivery report has
        ///     not yet been handled and a number which is less than or equal to the
        ///     number of pending delivery report callback events (as determined by
        ///     an internal librdkafka implementation detail).
        /// </returns>
        /// <remarks>
        ///     This method should typically be called prior to destroying a producer
        ///     instance to make sure all queued and in-flight produce requests are
        ///     completed before terminating. The wait time is bounded by the
        ///     millisecondsTimeout parameter.
        ///
        ///     A related configuration parameter is message.timeout.ms which determines
        ///     the maximum length of time librdkafka attempts to deliver a message 
        ///     before giving up and so also affects the maximum time a call to Flush 
        ///     may block.
        /// </remarks>
        public int Flush(int millisecondsTimeout)
            => kafkaHandle.Flush(millisecondsTimeout);

        /// <summary>
        ///     Wait until all outstanding produce requests and delievery report
        ///     callbacks are completed. Refer to <see cref="Flush(int)" /> for
        ///     more information.
        /// 
        ///     [UNSTABLE-API] - the semantics and/or type of the return value is
        ///     subject to change.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum length of time to block. You should typically use a
        ///     relatively short timout period because this operation cannot be 
        ///     cancelled.
        /// </param>
        /// <returns>
        ///     The current librdkafka out queue length. Refer to <see cref="Flush(int)" />
        ///     for more information.
        /// </returns>
        public int Flush(TimeSpan timeout)
            => kafkaHandle.Flush(timeout.TotalMillisecondsAsInt());

        /// <summary>
        ///     Equivalent to <see cref="Flush(int)" /> with infinite timeout.
        ///
        ///     [UNSTABLE-API] - the semantics and/or type of the return value is
        ///     subject to change.
        /// </summary>
        /// <returns>
        ///     Refer to <see cref="Flush(int)" />.
        /// </returns>
        [Obsolete("Use an overload of Flush with a finite timeout.", false)]
        public int Flush()
            => kafkaHandle.Flush(-1);

        /// <summary>
        ///     Releases all resources used by this Producer.
        /// </summary>
        /// <remarks>
        ///     You will often want to call <see cref="Flush()" />
        ///     before disposing a Producer instance.
        /// </remarks>
        public void Dispose()
        {
            foreach (var kv in topicHandles)
            {
                kv.Value.Dispose();
            }

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

        /// <summary>
        ///     Get information pertaining to all groups in the Kafka cluster (blocking)
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

        private Metadata GetMetadata(bool allTopics, string topic, int millisecondsTimeout)
            => kafkaHandle.GetMetadata(allTopics, topic == null ? null : getKafkaTopicHandle(topic), millisecondsTimeout);

        /// <summary>
        ///     Query the cluster for metadata (blocking).
        ///
        ///     - allTopics = true - request all topics from cluster
        ///     - allTopics = false, topic = null - request only locally known topics.
        ///     - allTopics = false, topic = valid - request specific topic
        ///
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        public Metadata GetMetadata(bool allTopics, string topic, TimeSpan timeout)
            => GetMetadata(allTopics, topic, timeout.TotalMillisecondsAsInt());

        /// <summary>
        ///     Refer to <see cref="GetMetadata(bool,string,TimeSpan)" />
        /// 
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        public Metadata GetMetadata(bool allTopics, string topic)
            => GetMetadata(allTopics, topic, -1);

        /// <summary>
        ///     Refer to <see cref="GetMetadata(bool,string,TimeSpan)" />
        /// 
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        public Metadata GetMetadata()
            => GetMetadata(true, null, -1);

        /// <summary>
        ///     Adds one or more brokers to the Producer's list of initial
        ///     bootstrap brokers. 
        ///
        ///     Note: Additional brokers are discovered automatically as 
        ///     soon as the Producer connects to any broker by querying the 
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


    internal class SerializingProducer<TKey, TValue> : ISerializingProducer<TKey, TValue>
    {
        protected readonly Producer producer;

        public ISerializer<TKey> KeySerializer { get; }

        public ISerializer<TValue> ValueSerializer { get; }

        public SerializingProducer(Producer producer, ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
        {
            this.producer = producer;
            KeySerializer = keySerializer;
            ValueSerializer = valueSerializer;

            if (keySerializer != null && keySerializer == valueSerializer)
            {
                throw new ArgumentException("Key and value serializers must not be the same object.");
            }

            if (KeySerializer == null)
            {
                if (typeof(TKey) != typeof(Null))
                {
                    throw new ArgumentNullException("Key serializer must be specified.");
                }
            }

            if (ValueSerializer == null)
            {
                if (typeof(TValue) != typeof(Null))
                {
                    throw new ArgumentNullException("Value serializer must be specified.");
                }
            }
        }

        private class TypedTaskDeliveryHandlerShim : TaskCompletionSource<Message<TKey, TValue>>, IDeliveryHandler
        {
            public TypedTaskDeliveryHandlerShim(TKey key, TValue val)
#if !NET45
                : base(TaskCreationOptions.RunContinuationsAsynchronously)
#endif
            {
                Key = key;
                Value = val;
            }

            public TKey Key;

            public TValue Value;

            public bool MarshalData { get { return false; } }

            public void HandleDeliveryReport(Message message)
            {
                var mi = new Message<TKey, TValue>(
                    message.Topic,
                    message.Partition,
                    message.Offset,
                    Key,
                    Value,
                    message.Timestamp,
                    message.Error
                );

#if NET45
                System.Threading.Tasks.Task.Run(() => SetResult(mi));
#else
                SetResult(mi);
#endif
            }
        }

        private Task<Message<TKey, TValue>> Produce(string topic, TKey key, TValue val, DateTime? timestamp, int partition, bool blockIfQueueFull)
        {
            var handler = new TypedTaskDeliveryHandlerShim(key, val);
            var keyBytes = KeySerializer?.Serialize(topic, key);
            var valBytes = ValueSerializer?.Serialize(topic, val);
            producer.Produce(topic, valBytes, 0, valBytes == null ? 0 : valBytes.Length, keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length, timestamp, partition, blockIfQueueFull, handler);
            return handler.Task;
        }

        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val)
            => Produce(topic, key, val, null, Producer.RD_KAFKA_PARTITION_UA, true);

        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, int partition, bool blockIfQueueFull)
            => Produce(topic, key, val, null, partition, blockIfQueueFull);

        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, int partition)
            => Produce(topic, key, val, null, partition, true);

        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, bool blockIfQueueFull)
            => Produce(topic, key, val, null, Producer.RD_KAFKA_PARTITION_UA, blockIfQueueFull);


        private class TypedDeliveryHandlerShim : IDeliveryHandler
        {
            public TypedDeliveryHandlerShim(TKey key, TValue val, IDeliveryHandler<TKey, TValue> handler)
            {
                Key = key;
                Value = val;
                Handler = handler;
            }

            public TKey Key;

            public TValue Value;

            public bool MarshalData { get { return false; } }

            public IDeliveryHandler<TKey, TValue> Handler;

            public void HandleDeliveryReport(Message message)
            {
                Handler.HandleDeliveryReport(new Message<TKey, TValue>(
                    message.Topic,
                    message.Partition,
                    message.Offset,
                    Key,
                    Value,
                    message.Timestamp,
                    message.Error
                ));
            }
        }

        private void Produce(string topic, TKey key, TValue val, DateTime? timestamp, int partition, bool blockIfQueueFull, IDeliveryHandler<TKey, TValue> deliveryHandler)
        {
            var handler = new TypedDeliveryHandlerShim(key, val, deliveryHandler);
            var keyBytes = KeySerializer?.Serialize(topic, key);
            var valBytes = ValueSerializer?.Serialize(topic, val);
            producer.Produce(topic, valBytes, 0, valBytes == null ? 0 : valBytes.Length, keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length, timestamp, partition, blockIfQueueFull, handler);
        }

        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => Produce(topic, key, val, null, Producer.RD_KAFKA_PARTITION_UA, true, deliveryHandler);

        public void ProduceAsync(string topic, TKey key, TValue val, int partition, bool blockIfQueueFull, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => Produce(topic, key, val, null, partition, blockIfQueueFull, deliveryHandler);

        public void ProduceAsync(string topic, TKey key, TValue val, int partition, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => Produce(topic, key, val, null, partition, true, deliveryHandler);

        public void ProduceAsync(string topic, TKey key, TValue val, bool blockIfQueueFull, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => Produce(topic, key, val, null, Producer.RD_KAFKA_PARTITION_UA, blockIfQueueFull, deliveryHandler);

        public string Name
            => producer.Name;
    }


    /// <summary>
    ///     Implements a high-level Apache Kafka producer with key
    ///     and value serialization.
    /// </summary>
    public class Producer<TKey, TValue> : ISerializingProducer<TKey, TValue>, IDisposable
    {
        private readonly Producer producer;
        private readonly ISerializingProducer<TKey, TValue> serializingProducer;

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
        /// <param name="manualPoll">
        ///     If true, does not start a dedicated polling thread to trigger events or receive delivery reports -
        ///     you must call the Poll method periodically instead. Typically you should set this parameter to false.
        /// </param>
        /// <param name="disableDeliveryReports">
        ///     If true, disables delivery report notification. Note: if set to true and you use a ProduceAsync variant that returns
        ///     a Task, the Tasks will never complete. Typically you should set this parameter to false. Set it to true for "fire and
        ///     forget" semantics and a small boost in performance.
        /// </param>
        private Producer(
            IEnumerable<KeyValuePair<string, object>> config,
            ISerializer<TKey> keySerializer,
            ISerializer<TValue> valueSerializer,
            bool manualPoll, bool disableDeliveryReports)
        {
            var configWithoutKeySerializerProperties = keySerializer?.Configure(config, true) ?? config;
            var configWithoutValueSerializerProperties = valueSerializer?.Configure(config, false) ?? config;

            var configWithoutSerializerProperties = config.Where(item => 
                configWithoutKeySerializerProperties.Any(ci => ci.Key == item.Key) &&
                configWithoutValueSerializerProperties.Any(ci => ci.Key == item.Key)
            );

            producer = new Producer(
                configWithoutSerializerProperties, 
                manualPoll, 
                disableDeliveryReports
            );

            serializingProducer = producer.GetSerializingProducer(keySerializer, valueSerializer);
        }

        /// <summary>
        ///     Initializes a new Producer instance.
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
            ISerializer<TValue> valueSerializer
        ) : this(config, keySerializer, valueSerializer, false, false) {}


        /// <summary>
        ///     Gets the ISerializer implementation instance used to serialize keys.
        /// </summary>
        public ISerializer<TKey> KeySerializer
            => serializingProducer.KeySerializer;

        /// <summary>
        ///     Gets the ISerializer implementation instance used to serialize values.
        /// </summary>
        public ISerializer<TValue> ValueSerializer
            => serializingProducer.ValueSerializer;

        /// <summary>
        ///     Gets the name of this producer instance.
        ///     Contains (but is not equal to) the client.id configuration parameter.
        /// </summary>
        /// <remarks>
        ///     This name will be unique across all producer instances
        ///     in a given application which allows log messages to be
        ///     associated with the corresponding instance.
        /// </remarks>
        public string Name
            => serializingProducer.Name;

        /// <summary>
        ///     Asynchronously send a single message to the broker.
        ///     Refer to <see cref="ProduceAsync(string, TKey, TValue, int, bool)" /> for more information.
        /// </summary>
        /// <remarks>
        ///     The partition the message is produced to is determined using the configured partitioner.
        ///     
        ///     Blocks if the send queue is full. Warning: if background polling is disabled and Poll is
        ///     not being called in another thread, this will block indefinitely.
        /// </remarks>
        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val)
            => serializingProducer.ProduceAsync(topic, key, val);

        /// <summary>
        ///     Asynchronously send a single message to the broker.
        /// </summary>
        /// <param name="topic">
        ///     The target topic.
        /// </param>
        /// <param name="partition">
        ///     The target partition (if -1, this is determined by the partitioner
        ///     configured for the topic).
        /// </param>
        /// <param name="key">
        ///     the message key (possibly null if allowed by the key serializer).
        /// </param>
        /// <param name="val">
        ///     the message value (possibly null if allowed by the value serializer).
        /// </param>
        /// <param name="blockIfQueueFull">
        ///     Whether or not to block if the send queue is full.
        ///     If false, a KafkaExcepion (with Error.Code == ErrorCode.Local_QueueFull) 
        ///     will be thrown if an attempt is made to produce a message
        ///     and the send queue is full.
        ///      
        ///     Warning: blockIfQueueFull is set to true, background polling is 
        ///     disabled and Poll is not being called in another thread, 
        ///     this will block indefinitely.
        /// </param>
        /// <returns>
        ///     A Task which will complete with the corresponding delivery report
        ///     for this request.
        /// </returns>
        /// <remarks>
        ///     If you require strict ordering of delivery reports to be maintained, 
        ///     you should use a variant of ProduceAsync that takes an IDeliveryHandler
        ///     parameter, not a variant that returns a Task&lt;Message&gt; because 
        ///     Tasks are completed on arbitrary thread pool threads and can 
        ///     be executed out of order.
        /// </remarks>
        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, int partition, bool blockIfQueueFull)
            => serializingProducer.ProduceAsync(topic, key, val, partition, blockIfQueueFull);

        /// <summary>
        ///     Asynchronously send a single message to the broker.
        ///     Refer to <see cref="ProduceAsync(string, TKey, TValue, int, bool)" /> for more information.
        /// </summary>
        /// <remarks>
        ///     Blocks if the send queue is full. Warning: if background polling is disabled and Poll is
        ///     not being called in another thread, this will block indefinitely.
        /// </remarks>
        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, int partition)
            => serializingProducer.ProduceAsync(topic, key, val, partition);

        /// <summary>
        ///     Asynchronously send a single message to the broker.
        ///     Refer to <see cref="ProduceAsync(string, TKey, TValue, int, bool)" /> for more information.
        /// </summary>
        /// <remarks>
        ///     The partition the message is produced to is determined using the configured partitioner.
        /// </remarks>
        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, bool blockIfQueueFull)
            => serializingProducer.ProduceAsync(topic, key, val, blockIfQueueFull);


        /// <summary>
        ///     Asynchronously send a single message to the broker (order of delivery reports strictly guarenteed).
        ///     See <see cref="ProduceAsync(string, TKey, TValue, int, bool, IDeliveryHandler{TKey, TValue})"/> for more information.
        /// </summary>
        /// <remarks>
        ///     The partition the message is produced to is determined using the configured partitioner.
        ///     
        ///     Blocks if the send queue is full. Warning: if background polling is disabled and Poll is
        ///     not being called in another thread, this will block indefinitely.
        /// </remarks>
        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => serializingProducer.ProduceAsync(topic, key, val, deliveryHandler);

        /// <summary>
        ///     Asynchronously send a single message to the broker (order of delivery reports strictly guarenteed).
        /// </summary>
        /// <remarks>
        ///     Notification of delivery reports is via an IDeliveryHandler instance. Use IDeliveryHandler variants of 
        ///     ProduceAsync if you require notification of delivery reports strictly in the order they were 
        ///     acknowledged by the broker / failed (failure may be via broker or local). IDeliveryHandler.HandleDeliveryReport
        ///     callbacks are executed on the Poll thread.
        ///     
        ///     Refer to <see cref="ProduceAsync(string, TKey, TValue, int, bool)" />
        ///     for more information.
        /// </remarks>
        public void ProduceAsync(string topic, TKey key, TValue val, int partition, bool blockIfQueueFull, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => serializingProducer.ProduceAsync(topic, key, val, partition, blockIfQueueFull, deliveryHandler);

        /// <summary>
        ///     Asynchronously send a single message to the broker (order of delivery reports strictly guarenteed).
        ///     See <see cref="ProduceAsync(string, TKey, TValue, int, bool, IDeliveryHandler{TKey, TValue})"/> for more information.
        /// </summary>
        /// <remarks>
        ///     Blocks if the send queue is full. Warning: if background polling is disabled and Poll is
        ///     not being called in another thread, this will block indefinitely.
        /// </remarks>
        public void ProduceAsync(string topic, TKey key, TValue val, int partition, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => serializingProducer.ProduceAsync(topic, key, val, partition, deliveryHandler);

        /// <summary>
        ///     Asynchronously send a single message to the broker (order of delivery reports strictly guarenteed).
        ///     See <see cref="ProduceAsync(string, TKey, TValue, int, bool, IDeliveryHandler{TKey, TValue})"/> for more information.
        /// </summary>
        /// <remarks>
        ///     The partition the message is produced to is determined using the configured partitioner.
        /// </remarks>
        public void ProduceAsync(string topic, TKey key, TValue val, bool blockIfQueueFull, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => serializingProducer.ProduceAsync(topic, key, val, blockIfQueueFull, deliveryHandler);


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
            add { producer.OnLog += value; }
            remove { producer.OnLog -= value; }
        }

        /// <summary>
        ///     Raised on librdkafka statistics events. JSON formatted
        ///     string as defined here: https://github.com/edenhill/librdkafka/wiki/Statistics
        /// </summary>
        /// <remarks>
        ///     You can enable statistics and set the statistics interval
        ///     using the statistics.interval.ms configuration parameter
        ///     (disabled by default).
        ///
        ///     Called on the Producer poll thread.
        /// </remarks>
        public event EventHandler<string> OnStatistics
        {
            add { producer.OnStatistics += value; }
            remove { producer.OnStatistics -= value; }
        }

        /// <summary>
        ///     Raised on critical errors, e.g. connection failures or all 
        ///     brokers down. Note that the client will try to automatically 
        ///     recover from errors - these errors should be seen as 
        ///     informational rather than catastrophic
        /// </summary>
        /// <remarks>
        ///     Called on the Producer poll thread.
        /// </remarks>
        public event EventHandler<Error> OnError
        {
            add { producer.OnError += value; }
            remove { producer.OnError -= value; }
        }


        /// <summary>
        ///     Wait until all outstanding produce requests and delievery report
        ///     callbacks are completed.
        /// 
        ///     [UNSTABLE-API] - the semantics and/or type of the return value is
        ///     subject to change.
        /// </summary>
        /// <param name="millisecondsTimeout">
        ///     The maximum time to block in milliseconds, or -1 to block
        ///     indefinitely. You should typically use a relatively short timout 
        ///     period because this operation cannot be cancelled.
        /// </param>
        /// <returns>
        ///     The current librdkafka out queue length. This should be interpreted
        ///     as a rough indication of the number of messages waiting to be sent
        ///     to or acknowledged by the broker. If zero, there are no outstanding
        ///     messages or callbacks. Specifically, the value is equal to the sum
        ///     of the number of produced messages for which a delivery report has
        ///     not yet been handled and a number which is less than or equal to the
        ///     number of pending delivery report callback events (as determined by
        ///     an internal librdkafka implementation detail).
        /// </returns>
        /// <remarks>
        ///     This method should typically be called prior to destroying a producer
        ///     instance to make sure all queued and in-flight produce requests are
        ///     completed before terminating. The wait time is bounded by the
        ///     millisecondsTimeout parameter.
        ///
        ///     A related topic configuration parameter is message.timeout.ms which 
        ///     determines the maximum length of time librdkafka attempts to deliver
        ///     a message before giving up and so also affects the maximum time a call
        ///     to Flush may block.
        /// </remarks>
        public int Flush(int millisecondsTimeout)
            => producer.Flush(millisecondsTimeout);

        /// <summary>
        ///     Wait until all outstanding produce requests and delievery report
        ///     callbacks are completed. Refer to <see cref="Flush(int)" /> for
        ///     more information.
        /// 
        ///     [UNSTABLE-API] - the semantics and/or type of the return value is
        ///     subject to change.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum length of time to block. You should typically use a
        ///     relatively short timout period because this operation cannot be 
        ///     cancelled.
        /// </param>
        /// <returns>
        ///     The current librdkafka out queue length. Refer to <see cref="Flush(int)" />
        ///     for more information.
        /// </returns>
        public int Flush(TimeSpan timeout)
            => producer.Flush(timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     Releases all resources used by this Producer.
        /// </summary>
        /// <remarks>
        ///     You will often want to call <see cref="Flush(int)" />
        ///     before disposing a Producer instance.
        /// </remarks>
        public void Dispose()
        {
            if (KeySerializer != null)
            {
                KeySerializer.Dispose();
            }

            if (ValueSerializer != null)
            {
                ValueSerializer.Dispose();
            }

            producer.Dispose();
        }


        /// <summary>
        ///     Get information pertaining to all groups in the Kafka cluster (blocking)
        ///
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
        /// </param>
        public List<GroupInfo> ListGroups(TimeSpan timeout)
            => producer.ListGroups(timeout);


        /// <summary>
        ///     Get information pertaining to a particular group in the
        ///     Kafka cluster (blocks)
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
            => producer.ListGroup(group, timeout);

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
            => producer.ListGroup(group);


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
            => producer.QueryWatermarkOffsets(topicPartition, timeout);

        /// <summary>
        ///     Query the Kafka cluster for low (oldest/beginning) and high (newest/end)
        ///     offsets for the specified topic/partition (blocks, potentialy indefinitely).
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
            => producer.QueryWatermarkOffsets(topicPartition);


        /// <summary>
        ///     Refer to Producer.GetMetadata(bool, string, TimeSpan) for more information.
        ///     
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        public Metadata GetMetadata(bool allTopics, string topic, TimeSpan timeout)
            => producer.GetMetadata(allTopics, topic, timeout);

        /// <summary>
        ///     Refer to Producer.GetMetadata(bool, string) for more information
        ///     
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        public Metadata GetMetadata(bool allTopics, string topic)
            => producer.GetMetadata(allTopics, topic);

        /// <summary>
        ///     Adds one or more brokers to the Producer's list of initial
        ///     bootstrap brokers. 
        ///
        ///     Note: Additional brokers are discovered automatically as 
        ///     soon as the Producer connects to any broker by querying the 
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
            => producer.AddBrokers(brokers);
    }
}
