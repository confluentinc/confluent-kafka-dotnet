// Copyright 2016-2018 Confluent Inc.
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
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Serdes;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;


namespace Confluent.Kafka
{
    /// <summary>
    ///     A high level producer with serialization capability.
    /// </summary>
    public class Producer<TKey, TValue> : IProducer<TKey, TValue>, IClient
    {
        internal class Config
        {
            public IEnumerable<KeyValuePair<string, string>> config;
            public Action<Error> errorHandler;
            public Action<LogMessage> logHandler;
            public Action<string> statisticsHandler;
        }

        private ISerializer<TKey> keySerializer;
        private ISerializer<TValue> valueSerializer;
        private IAsyncSerializer<TKey> asyncKeySerializer;
        private IAsyncSerializer<TValue> asyncValueSerializer;

        private static readonly Dictionary<Type, object> defaultSerializers = new Dictionary<Type, object>
        {
            { typeof(Null), Serializers.Null },
            { typeof(int), Serializers.Int32 },
            { typeof(long), Serializers.Int64 },
            { typeof(string), Serializers.Utf8 },
            { typeof(float), Serializers.Single },
            { typeof(double), Serializers.Double },
            { typeof(byte[]), Serializers.ByteArray }
        };

        private int cancellationDelayMaxMs;
        private bool disposeHasBeenCalled = false;
        private object disposeHasBeenCalledLockObj = new object();

        private bool manualPoll = false;
        private bool enableDeliveryReports = true;
        private bool enableDeliveryReportKey = true;
        private bool enableDeliveryReportValue = true;
        private bool enableDeliveryReportTimestamp = true;
        private bool enableDeliveryReportHeaders = true;
        private bool enableDeliveryReportPersistedStatus = true;

        private SafeKafkaHandle ownedKafkaHandle;
        private Handle borrowedHandle;

        private SafeKafkaHandle KafkaHandle
            => ownedKafkaHandle != null 
                ? ownedKafkaHandle
                : borrowedHandle.LibrdkafkaHandle;

        private Task callbackTask;
        private CancellationTokenSource callbackCts;

        private Task StartPollTask(CancellationToken ct)
            => Task.Factory.StartNew(() =>
                {
                    try
                    {
                        while (true)
                        {
                            ct.ThrowIfCancellationRequested();
                            ownedKafkaHandle.Poll((IntPtr)cancellationDelayMaxMs);
                        }
                    }
                    catch (OperationCanceledException) {}
                }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);


        private Action<Error> errorHandler;
        private Librdkafka.ErrorDelegate errorCallbackDelegate;
        private void ErrorCallback(IntPtr rk, ErrorCode err, string reason, IntPtr opaque)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (ownedKafkaHandle.IsClosed) { return; }
            errorHandler?.Invoke(KafkaHandle.CreatePossiblyFatalError(err, reason));
        }


        private Action<string> statisticsHandler;
        private Librdkafka.StatsDelegate statisticsCallbackDelegate;
        private int StatisticsCallback(IntPtr rk, IntPtr json, UIntPtr json_len, IntPtr opaque)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (ownedKafkaHandle.IsClosed) { return 0; }
            statisticsHandler?.Invoke(Util.Marshal.PtrToStringUTF8(json));
            return 0; // instruct librdkafka to immediately free the json ptr.
        }


        private Action<LogMessage> logHandler;
        private object loggerLockObj = new object();
        private Librdkafka.LogDelegate logCallbackDelegate;
        private void LogCallback(IntPtr rk, SyslogLevel level, string fac, string buf)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            // Note: kafkaHandle can be null if the callback is during construction (in that case, we want the delegate to run).
            if (ownedKafkaHandle != null && ownedKafkaHandle.IsClosed) { return; }
            logHandler?.Invoke(new LogMessage(Util.Marshal.PtrToStringUTF8(Librdkafka.name(rk)), level, fac, buf));
        }

        private Librdkafka.DeliveryReportDelegate DeliveryReportCallback;

        private void DeliveryReportCallbackImpl(IntPtr rk, IntPtr rkmessage, IntPtr opaque)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (ownedKafkaHandle.IsClosed) { return; }

            var msg = Util.Marshal.PtrToStructure<rd_kafka_message>(rkmessage);

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
                        byte[] headerValue = null;
                        if (valuep != IntPtr.Zero)
                        {
                            headerValue = new byte[(int)sizep];
                            Marshal.Copy(valuep, headerValue, 0, (int)sizep);
                        }
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

            PersistenceStatus messageStatus = PersistenceStatus.PossiblyPersisted;
            if (enableDeliveryReportPersistedStatus)
            {
                messageStatus = Librdkafka.message_status(rkmessage);
            }

            deliveryHandler.HandleDeliveryReport(
                new DeliveryReport<Null, Null>
                {
                    // Topic is not set here in order to avoid the marshalling cost.
                    // Instead, the delivery handler is expected to cache the topic string.
                    Partition = msg.partition, 
                    Offset = msg.offset, 
                    Error = KafkaHandle.CreatePossiblyFatalError(msg.err, null),
                    Status = messageStatus,
                    Message = new Message<Null, Null> { Timestamp = new Timestamp(timestamp, (TimestampType)timestampType), Headers = headers }
                }
            );
        }

        private void ProduceImpl(
            string topic,
            byte[] val, int valOffset, int valLength,
            byte[] key, int keyOffset, int keyLength,
            Timestamp timestamp,
            Partition partition,
            IEnumerable<IHeader> headers,
            IDeliveryHandler deliveryHandler)
        {
            if (timestamp.Type != TimestampType.CreateTime)
            {
                if (timestamp != Timestamp.Default)
                {
                    throw new ArgumentException("Timestamp must be either Timestamp.Default, or Timestamp.CreateTime.");
                }
            }

            if (this.enableDeliveryReports && deliveryHandler != null)
            {
                // Passes the TaskCompletionSource to the delivery report callback via the msg_opaque pointer

                // Note: There is a level of indirection between the GCHandle and
                // physical memory address. GCHandle.ToIntPtr doesn't get the
                // physical address, it gets an id that refers to the object via
                // a handle-table.
                var gch = GCHandle.Alloc(deliveryHandler);
                var ptr = GCHandle.ToIntPtr(gch);

                var err = KafkaHandle.Produce(
                    topic,
                    val, valOffset, valLength,
                    key, keyOffset, keyLength,
                    partition.Value,
                    timestamp.UnixTimestampMs,
                    headers,
                    ptr);

                if (err != ErrorCode.NoError)
                {
                    gch.Free();
                    throw new KafkaException(KafkaHandle.CreatePossiblyFatalError(err, null));
                }
            }
            else
            {
                var err = KafkaHandle.Produce(
                    topic,
                    val, valOffset, valLength,
                    key, keyOffset, keyLength,
                    partition.Value,
                    timestamp.UnixTimestampMs,
                    headers,
                    IntPtr.Zero);

                if (err != ErrorCode.NoError)
                {
                    throw new KafkaException(KafkaHandle.CreatePossiblyFatalError(err, null));
                }
            }
        }


        /// <summary>
        ///     Poll for callback events. Typically, you should not 
        ///     call this method. Only call on producer instances 
        ///     where background polling has been disabled.
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
        {
            if (!manualPoll)
            {
                throw new InvalidOperationException("Poll method called, but manual polling is not enabled.");
            }

            return this.KafkaHandle.Poll((IntPtr)timeout.TotalMillisecondsAsInt());
        }


        /// <summary>
        ///     Wait until all outstanding produce requests and delievery report
        ///     callbacks are completed.
        ///    
        ///     [API-SUBJECT-TO-CHANGE] - the semantics and/or type of the return value
        ///     is subject to change.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum length of time to block. You should typically use a
        ///     relatively short timout period and loop until the return value
        ///     becomes zero because this operation cannot be cancelled. 
        /// </param>
        /// <returns>
        ///     The current librdkafka out queue length. This should be interpreted
        ///     as a rough indication of the number of messages waiting to be sent
        ///     to or acknowledged by the broker. If zero, there are no outstanding
        ///     messages or callbacks. Specifically, the value is equal to the sum
        ///     of the number of produced messages for which a delivery report has
        ///     not yet been handled and a number which is less than or equal to the
        ///     number of pending delivery report callback events (as determined by
        ///     the number of outstanding protocol requests).
        /// </returns>
        /// <remarks>
        ///     This method should typically be called prior to destroying a producer
        ///     instance to make sure all queued and in-flight produce requests are
        ///     completed before terminating. The wait time is bounded by the
        ///     timeout parameter.
        ///    
        ///     A related configuration parameter is message.timeout.ms which determines
        ///     the maximum length of time librdkafka attempts to deliver a message 
        ///     before giving up and so also affects the maximum time a call to Flush 
        ///     may block.
        /// 
        ///     Where this Producer instance shares a Handle with one or more other
        ///     producer instances, the Flush method will wait on messages produced by
        ///     the other producer instances as well.
        /// </remarks>
        public int Flush(TimeSpan timeout)
            => KafkaHandle.Flush(timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     Wait until all outstanding produce requests and delievery report
        ///     callbacks are completed.
        /// </summary>
        /// <remarks>
        ///     This method should typically be called prior to destroying a producer
        ///     instance to make sure all queued and in-flight produce requests are
        ///     completed before terminating. 
        ///    
        ///     A related configuration parameter is message.timeout.ms which determines
        ///     the maximum length of time librdkafka attempts to deliver a message 
        ///     before giving up and so also affects the maximum time a call to Flush 
        ///     may block.
        /// 
        ///     Where this Producer instance shares a Handle with one or more other
        ///     producer instances, the Flush method will wait on messages produced by
        ///     the other producer instances as well.
        /// </remarks>
        /// <exception cref="System.OperationCanceledException">
        ///     Thrown if the operation is cancelled.
        /// </exception>
        public void Flush(CancellationToken cancellationToken)
        {
            while (true)
            {
                int result = KafkaHandle.Flush(100);
                if (result == 0)
                {
                    return;
                }
                if (cancellationToken.IsCancellationRequested)
                {
                    // TODO: include flush number in exception.
                    throw new OperationCanceledException();
                }
            }
        }

        
        /// <summary>
        ///     Releases all resources used by this <see cref="Producer{TKey,TValue}" />.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        /// <summary>
        ///     Releases the unmanaged resources used by the
        ///     <see cref="Producer{TKey,TValue}" />
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

            // do nothing if we borrowed a handle.
            if (ownedKafkaHandle == null) { return; }

            if (disposing)
            {
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

                // calls to rd_kafka_destroy may result in callbacks
                // as a side-effect. however the callbacks this class
                // registers with librdkafka ensure that any registered
                // events are not called if kafkaHandle has been closed.
                // this avoids deadlocks in common scenarios.
                ownedKafkaHandle.Dispose();
            }
        }


        /// <summary>
        ///     <see cref="IClient.Name" />
        /// </summary>
        public string Name
            => KafkaHandle.Name;


        /// <summary>
        ///     <see cref="IClient.AddBrokers(string)" />
        /// </summary>
        public int AddBrokers(string brokers)
            => KafkaHandle.AddBrokers(brokers);


        /// <summary>
        ///     <see cref="IClient.Handle" />
        /// </summary>
        public Handle Handle 
        {
            get
            {
                if (this.ownedKafkaHandle != null)
                {
                    return new Handle { Owner = this, LibrdkafkaHandle = ownedKafkaHandle };
                }
                
                return borrowedHandle;
            }
        }

        private void InitializeSerializers(
            ISerializer<TKey> keySerializer,
            ISerializer<TValue> valueSerializer,
            IAsyncSerializer<TKey> asyncKeySerializer,
            IAsyncSerializer<TValue> asyncValueSerializer)
        {
            // setup key serializer.
            if (keySerializer == null && asyncKeySerializer == null)
            {
                if (!defaultSerializers.TryGetValue(typeof(TKey), out object serializer))
                {
                    throw new ArgumentNullException(
                        $"Key serializer not specified and there is no default serializer defined for type {typeof(TKey).Name}.");
                }
                this.keySerializer = (ISerializer<TKey>)serializer;
            }
            else if (keySerializer == null && asyncKeySerializer != null)
            {
                this.asyncKeySerializer = asyncKeySerializer;
            }
            else if (keySerializer != null && asyncKeySerializer == null)
            {
                this.keySerializer = keySerializer;
            }
            else
            {
                throw new InvalidOperationException("FATAL: Both async and sync key serializers were set.");
            }

            // setup value serializer.
            if (valueSerializer == null && asyncValueSerializer == null)
            {
                if (!defaultSerializers.TryGetValue(typeof(TValue), out object serializer))
                {
                    throw new ArgumentNullException(
                        $"Value serializer not specified and there is no default serializer defined for type {typeof(TKey).Name}.");
                }
                this.valueSerializer = (ISerializer<TValue>)serializer;
            }
            else if (valueSerializer == null && asyncValueSerializer != null)
            {
                this.asyncValueSerializer = asyncValueSerializer;
            }
            else if (valueSerializer != null && asyncValueSerializer == null)
            {
                this.valueSerializer = valueSerializer;
            }
            else
            {
                throw new InvalidOperationException("FATAL: Both async and sync value serializers were set.");
            }
        }

        internal Producer(DependentProducerBuilder<TKey, TValue> builder)
        {
            this.borrowedHandle = builder.Handle;

            if (!borrowedHandle.Owner.GetType().Name.Contains("Producer")) // much simpler than checking actual types.
            {
                throw new Exception("A Producer instance may only be constructed using the handle of another Producer instance.");
            }

            InitializeSerializers(
                builder.KeySerializer, builder.ValueSerializer,
                builder.AsyncKeySerializer, builder.AsyncValueSerializer);
        }

        internal Producer(ProducerBuilder<TKey, TValue> builder)
        {
            var baseConfig = builder.ConstructBaseConfig(this);

            // TODO: Make Tasks auto complete when EnableDeliveryReportsPropertyName is set to false.
            // TODO: Hijack the "delivery.report.only.error" configuration parameter and add functionality to enforce that Tasks 
            //       that never complete are never created when this is set to true.

            this.statisticsHandler = baseConfig.statisticsHandler;
            this.logHandler = baseConfig.logHandler;
            this.errorHandler = baseConfig.errorHandler;

            var config = Confluent.Kafka.Config.ExtractCancellationDelayMaxMs(baseConfig.config, out this.cancellationDelayMaxMs);

            this.DeliveryReportCallback = DeliveryReportCallbackImpl;

            Librdkafka.Initialize(null);

            var modifiedConfig = config
                .Where(prop => 
                    prop.Key != ConfigPropertyNames.Producer.EnableBackgroundPoll &&
                    prop.Key != ConfigPropertyNames.Producer.EnableDeliveryReports &&
                    prop.Key != ConfigPropertyNames.Producer.DeliveryReportFields);

            if (modifiedConfig.Where(obj => obj.Key == "delivery.report.only.error").Count() > 0)
            {
                // A managed object is kept alive over the duration of the produce request. If there is no
                // delivery report generated, there will be a memory leak. We could possibly support this 
                // property by keeping track of delivery reports in managed code, but this seems like 
                // more trouble than it's worth.
                throw new ArgumentException("The 'delivery.report.only.error' property is not supported by this client");
            }

            var enableBackgroundPollObj = config.FirstOrDefault(prop => prop.Key == ConfigPropertyNames.Producer.EnableBackgroundPoll).Value;
            if (enableBackgroundPollObj != null)
            {
                this.manualPoll = !bool.Parse(enableBackgroundPollObj.ToString());
            }

            var enableDeliveryReportsObj = config.FirstOrDefault(prop => prop.Key == ConfigPropertyNames.Producer.EnableDeliveryReports).Value;
            if (enableDeliveryReportsObj != null)
            {
                this.enableDeliveryReports = bool.Parse(enableDeliveryReportsObj.ToString());
            }

            var deliveryReportEnabledFieldsObj = config.FirstOrDefault(prop => prop.Key == ConfigPropertyNames.Producer.DeliveryReportFields).Value;
            if (deliveryReportEnabledFieldsObj != null)
            {
                var fields = deliveryReportEnabledFieldsObj.ToString().Replace(" ", "");
                if (fields != "all")
                {
                    this.enableDeliveryReportKey = false;
                    this.enableDeliveryReportValue = false;
                    this.enableDeliveryReportHeaders = false;
                    this.enableDeliveryReportTimestamp = false;
                    this.enableDeliveryReportPersistedStatus = false;
                    if (fields != "none")
                    {
                        var parts = fields.Split(',');
                        foreach (var part in parts)
                        {
                            switch (part)
                            {
                                case "key": this.enableDeliveryReportKey = true; break;
                                case "value": this.enableDeliveryReportValue = true; break;
                                case "timestamp": this.enableDeliveryReportTimestamp = true; break;
                                case "headers": this.enableDeliveryReportHeaders = true; break;
                                case "status": this.enableDeliveryReportPersistedStatus = true; break;
                                default: throw new ArgumentException(
                                    $"Unknown delivery report field name '{part}' in config value '{ConfigPropertyNames.Producer.DeliveryReportFields}'.");
                            }
                        }
                    }
                }
            }

            var configHandle = SafeConfigHandle.Create();

            modifiedConfig.ToList().ForEach((kvp) => {
                if (kvp.Value == null) throw new ArgumentNullException($"'{kvp.Key}' configuration parameter must not be null.");
                configHandle.Set(kvp.Key, kvp.Value.ToString());
            });


            IntPtr configPtr = configHandle.DangerousGetHandle();

            if (enableDeliveryReports)
            {
                Librdkafka.conf_set_dr_msg_cb(configPtr, DeliveryReportCallback);
            }

            // Explicitly keep references to delegates so they are not reclaimed by the GC.
            errorCallbackDelegate = ErrorCallback;
            logCallbackDelegate = LogCallback;
            statisticsCallbackDelegate = StatisticsCallback;

            Librdkafka.conf_set_error_cb(configPtr, errorCallbackDelegate);
            Librdkafka.conf_set_log_cb(configPtr, logCallbackDelegate);
            Librdkafka.conf_set_stats_cb(configPtr, statisticsCallbackDelegate);

            this.ownedKafkaHandle = SafeKafkaHandle.Create(RdKafkaType.Producer, configPtr, this);
            configHandle.SetHandleAsInvalid(); // config object is no longer useable.

            if (!manualPoll)
            {
                callbackCts = new CancellationTokenSource();
                callbackTask = StartPollTask(callbackCts.Token);
            }

            InitializeSerializers(
                builder.KeySerializer, builder.ValueSerializer,
                builder.AsyncKeySerializer, builder.AsyncValueSerializer);
        }


        /// <summary>
        ///     Asynchronously send a single message to a Kafka topic/partition.
        /// </summary>
        /// <param name="topicPartition">
        ///     The topic/partition to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
        /// </param>
        /// <returns>
        ///     A Task which will complete with a delivery report corresponding to
        ///     the produce request, or an exception if an error occured.
        /// </returns>
        public async Task<DeliveryResult<TKey, TValue>> ProduceAsync(
            TopicPartition topicPartition,
            Message<TKey, TValue> message)
        {
            byte[] keyBytes;
            try
            {
                keyBytes = (keySerializer != null)
                    ? keySerializer.Serialize(message.Key, new SerializationContext(MessageComponentType.Key, topicPartition.Topic))
                    : await asyncKeySerializer.SerializeAsync(message.Key, new SerializationContext(MessageComponentType.Key, topicPartition.Topic));
            }
            catch (Exception exception)
            {
                throw new ProduceException<TKey, TValue>(
                    new Error(ErrorCode.Local_KeySerialization),
                    new DeliveryResult<TKey, TValue>
                    {
                        Message = message,
                        TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Unset)
                    },
                    exception);
            }

            byte[] valBytes;
            try
            {
                valBytes = (valueSerializer != null)
                    ? valueSerializer.Serialize(message.Value, new SerializationContext(MessageComponentType.Value, topicPartition.Topic))
                    : await asyncValueSerializer.SerializeAsync(message.Value, new SerializationContext(MessageComponentType.Value, topicPartition.Topic));
            }
            catch (Exception exception)
            {
                throw new ProduceException<TKey, TValue>(
                    new Error(ErrorCode.Local_ValueSerialization),
                    new DeliveryResult<TKey, TValue>
                    {
                        Message = message,
                        TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Unset)
                    },
                    exception);
            }

            try
            {
                if (enableDeliveryReports)
                {
                    var handler = new TypedTaskDeliveryHandlerShim<TKey, TValue>(
                        topicPartition.Topic,
                        enableDeliveryReportKey ? message.Key : default(TKey),
                        enableDeliveryReportValue ? message.Value : default(TValue));

                    ProduceImpl(
                        topicPartition.Topic,
                        valBytes, 0, valBytes == null ? 0 : valBytes.Length,
                        keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length,
                        message.Timestamp, topicPartition.Partition, message.Headers,
                        handler);

                    return await handler.Task;
                }
                else
                {
                    ProduceImpl(
                        topicPartition.Topic, 
                        valBytes, 0, valBytes == null ? 0 : valBytes.Length, 
                        keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length, 
                        message.Timestamp, topicPartition.Partition, message.Headers, 
                        null);

                    var result = new DeliveryResult<TKey, TValue>
                    {
                        TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Unset),
                        Message = message
                    };

                    return result;
                }
            }
            catch (KafkaException ex)
            {
                throw new ProduceException<TKey, TValue>(
                    ex.Error,
                    new DeliveryResult<TKey, TValue>
                    {
                        Message = message,
                        TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Unset)
                    });
            }

            // want other exceptions: ArgumentException, InvalidOperationException to propagate up.
        }


        /// <summary>
        ///     Asynchronously send a single message to a Kafka topic.
        ///     The partition the message is sent to is determined by
        ///     the partitioner defined using the 'partitioner' 
        ///     configuration property.
        /// </summary>
        /// <param name="topic">
        ///     The topic to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
        /// </param>
        /// <returns>
        ///     A Task which will complete with a delivery report corresponding to
        ///     the produce request, or an exception if an error occured.
        /// </returns>
        public Task<DeliveryResult<TKey, TValue>> ProduceAsync(
            string topic,
            Message<TKey, TValue> message
        )
            => ProduceAsync(new TopicPartition(topic, Partition.Any), message);


        /// <summary>
        ///     Asynchronously send a single message to a Kafka topic.
        ///     The partition the message is sent to is determined by
        ///     the partitioner defined using the 'partitioner' 
        ///     configuration property.
        /// </summary>
        /// <param name="topic">
        ///     The topic to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
        /// </param>
        /// <param name="deliveryHandler">
        ///     A delegate that will be called with a delivery report corresponding
        ///     to the produce request (if enabled).
        /// </param>
        public void BeginProduce(
            string topic,
            Message<TKey, TValue> message,
            Action<DeliveryReport<TKey, TValue>> deliveryHandler = null
        )
            => BeginProduce(new TopicPartition(topic, Partition.Any), message, deliveryHandler);


        /// <summary>
        ///     Asynchronously send a single message to a Kafka topic/partition.
        /// </summary>
        /// <param name="topicPartition">
        ///     The topic/partition to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
        /// </param>
        /// <param name="deliveryHandler">
        ///     A delegate that will be called with a delivery report corresponding
        ///     to the produce request (if enabled).
        /// </param>
        public void BeginProduce(
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            Action<DeliveryReport<TKey, TValue>> deliveryHandler = null)
        {
            if (deliveryHandler != null && !enableDeliveryReports)
            {
                throw new InvalidOperationException("A delivery handler was specified, but delivery reports are disabled.");
            }

            byte[] keyBytes;
            try
            {
                keyBytes = (keySerializer != null)
                    ? keySerializer.Serialize(message.Key, new SerializationContext(MessageComponentType.Key, topicPartition.Topic))
                    : Task.Run(async () => await asyncKeySerializer.SerializeAsync(message.Key, new SerializationContext(MessageComponentType.Key, topicPartition.Topic)))
                        .ConfigureAwait(false)
                        .GetAwaiter()
                        .GetResult();
            }
            catch (Exception ex)
            {
                throw new ProduceException<TKey, TValue>(
                    new Error(ErrorCode.Local_KeySerialization, ex.ToString()),
                    new DeliveryResult<TKey, TValue>
                    {
                        Message = message,
                        TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Unset),
                    }
                );
            }

            byte[] valBytes;
            try
            {
                valBytes = (valueSerializer != null)
                    ? valueSerializer.Serialize(message.Value, new SerializationContext(MessageComponentType.Value, topicPartition.Topic))
                    : Task.Run(async () => await asyncValueSerializer.SerializeAsync(message.Value, new SerializationContext(MessageComponentType.Value, topicPartition.Topic)))
                        .ConfigureAwait(continueOnCapturedContext: false)
                        .GetAwaiter()
                        .GetResult();
            }
            catch (Exception ex)
            {
                throw new ProduceException<TKey, TValue>(
                    new Error(ErrorCode.Local_ValueSerialization, ex.ToString()),
                    new DeliveryResult<TKey, TValue>
                    {
                        Message = message,
                        TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Unset),
                    }
                );
            }

            try
            {
                ProduceImpl(
                    topicPartition.Topic,
                    valBytes, 0, valBytes == null ? 0 : valBytes.Length, 
                    keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length, 
                    message.Timestamp, topicPartition.Partition, 
                    message.Headers,
                    new TypedDeliveryHandlerShim_Action<TKey, TValue>(
                        topicPartition.Topic,
                        enableDeliveryReportKey ? message.Key : default(TKey),
                        enableDeliveryReportValue ? message.Value : default(TValue),
                        deliveryHandler));
            }
            catch (KafkaException ex)
            {
                throw new ProduceException<TKey, TValue>(
                    ex.Error,
                    new DeliveryReport<TKey, TValue>
                        {
                            Message = message,
                            TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Unset)
                        });
            }

            // want other exceptions: ArgumentException, InvalidOperationException to propagate up.
        }

        private class TypedTaskDeliveryHandlerShim<K, V> : TaskCompletionSource<DeliveryResult<K, V>>, IDeliveryHandler
        {
            public TypedTaskDeliveryHandlerShim(string topic, K key, V val)
#if !NET45
                : base(TaskCreationOptions.RunContinuationsAsynchronously)
#endif
            {
                Topic = topic;
                Key = key;
                Value = val;
            }

            public string Topic;

            public K Key;

            public V Value;

            public void HandleDeliveryReport(DeliveryReport<Null, Null> deliveryReport)
            {
                if (deliveryReport == null)
                {
#if NET45
                    System.Threading.Tasks.Task.Run(() => TrySetResult(null));
#else
                    TrySetResult(null);
#endif
                    return;
                }

                var dr = new DeliveryResult<K, V>
                {
                    TopicPartitionOffset = deliveryReport.TopicPartitionOffset,
                    Status = deliveryReport.Status,
                    Message = new Message<K, V>
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
                if (deliveryReport.Error.IsError)
                {
                    System.Threading.Tasks.Task.Run(() => SetException(new ProduceException<K, V>(deliveryReport.Error, dr)));
                }
                else
                {
                    System.Threading.Tasks.Task.Run(() => TrySetResult(dr));
                }
#else
                if (deliveryReport.Error.IsError)
                {
                    TrySetException(new ProduceException<K, V>(deliveryReport.Error, dr));
                }
                else
                {
                    TrySetResult(dr);
                }
#endif
            }
        }

        private class TypedDeliveryHandlerShim_Action<K, V> : IDeliveryHandler
        {
            public TypedDeliveryHandlerShim_Action(string topic, K key, V val, Action<DeliveryReport<K, V>> handler)
            {
                Topic = topic;
                Key = key;
                Value = val;
                Handler = handler;
            }

            public string Topic;

            public K Key;

            public V Value;

            public Action<DeliveryReport<K, V>> Handler;

            public void HandleDeliveryReport(DeliveryReport<Null, Null> deliveryReport)
            {
                if (deliveryReport == null)
                {
                    return;
                }

                var dr = new DeliveryReport<K, V>
                {
                    TopicPartitionOffsetError = deliveryReport.TopicPartitionOffsetError,
                    Status = deliveryReport.Status,
                    Message = new Message<K, V> 
                    {
                        Key = Key,
                        Value = Value,
                        Timestamp = deliveryReport.Message == null 
                            ? new Timestamp(0, TimestampType.NotAvailable) 
                            : deliveryReport.Message.Timestamp,
                        Headers = deliveryReport.Message?.Headers
                    }
                };
                // topic is cached in this object, not set in the deliveryReport to avoid the 
                // cost of marshalling it.
                dr.Topic = Topic;

                if (Handler != null)
                {
                    Handler(dr);
                }
            }
        }
    }
}
