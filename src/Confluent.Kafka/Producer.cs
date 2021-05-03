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
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;


namespace Confluent.Kafka
{
    //TODO: rename producers files after review
    internal abstract class ProducerBase : IClient, ITransactionalProducer
    {
        internal class Config
        {
            public IEnumerable<KeyValuePair<string, string>> config;
            public Action<Error> errorHandler;
            public Action<LogMessage> logHandler;
            public Action<string> statisticsHandler;
            public Action<string> oAuthBearerTokenRefreshHandler;
            public Dictionary<string, PartitionerDelegate> partitioners;
            public PartitionerDelegate defaultPartitioner;
        }
        
        private int cancellationDelayMaxMs;
        private bool disposeHasBeenCalled = false;
        private object disposeHasBeenCalledLockObj = new object();

        private bool manualPoll = false;
        protected bool enableDeliveryReports = true;
        protected bool enableDeliveryReportKey = true;
        protected bool enableDeliveryReportValue = true;
        private bool enableDeliveryReportTimestamp = true;
        private bool enableDeliveryReportHeaders = true;
        private bool enableDeliveryReportPersistedStatus = true;

        private SafeKafkaHandle ownedKafkaHandle;
        private Handle borrowedHandle;

        private SafeKafkaHandle KafkaHandle
            => ownedKafkaHandle != null 
                ? ownedKafkaHandle
                : borrowedHandle.LibrdkafkaHandle;

        private List<GCHandle> partitionerHandles = new List<GCHandle>();

        private Task callbackTask;
        private CancellationTokenSource callbackCts;

        private int eventsServedCount = 0;
        private object pollSyncObj = new object();

        private Task StartPollTask(CancellationToken ct)
            => Task.Factory.StartNew(() =>
                {
                    try
                    {
                        while (true)
                        {
                            ct.ThrowIfCancellationRequested();
                            int eventsServedCount_ = ownedKafkaHandle.Poll((IntPtr)cancellationDelayMaxMs);
                            if (this.handlerException != null)
                            {
                                errorHandler?.Invoke(new Error(ErrorCode.Local_Application, handlerException.ToString()));
                                this.handlerException = null;
                            }

                            // note: lock {} is equivalent to Monitor.Enter then Monitor.Exit 
                            if (eventsServedCount_ > 0)
                            {
                                lock (pollSyncObj)
                                {
                                    this.eventsServedCount += eventsServedCount_;
                                    Monitor.Pulse(pollSyncObj);
                                }
                            }
                        }
                    }
                    catch (OperationCanceledException) {}
                }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);


        // .NET Exceptions are not propagated through native code, so we need to
        // do this book keeping explicitly.
        private Exception handlerException = null;


        private Action<Error> errorHandler;
        private Librdkafka.ErrorDelegate errorCallbackDelegate;
        private void ErrorCallback(IntPtr rk, ErrorCode err, string reason, IntPtr opaque)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (ownedKafkaHandle.IsClosed) { return; }

            try
            {
                errorHandler?.Invoke(KafkaHandle.CreatePossiblyFatalError(err, reason));
            }
            catch (Exception)
            {
                // Eat any exception thrown by user log handler code.
            }
        }


        private Action<string> statisticsHandler;
        private Librdkafka.StatsDelegate statisticsCallbackDelegate;
        private int StatisticsCallback(IntPtr rk, IntPtr json, UIntPtr json_len, IntPtr opaque)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (ownedKafkaHandle.IsClosed) { return 0; }

            try
            {
                statisticsHandler?.Invoke(Util.Marshal.PtrToStringUTF8(json));
            }
            catch (Exception e)
            {
                handlerException = e;
            }

            return 0; // instruct librdkafka to immediately free the json ptr.
        }

        private Action<string> oAuthBearerTokenRefreshHandler;
        private Librdkafka.OAuthBearerTokenRefreshDelegate oAuthBearerTokenRefreshCallbackDelegate;
        private void OAuthBearerTokenRefreshCallback(IntPtr rk, IntPtr oauthbearer_config, IntPtr opaque)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (ownedKafkaHandle.IsClosed) { return; }

            try
            {
                oAuthBearerTokenRefreshHandler?.Invoke(Util.Marshal.PtrToStringUTF8(oauthbearer_config));
            }
            catch (Exception e)
            {
                handlerException = e;
            }
        }


        private Action<LogMessage> logHandler;
        private object loggerLockObj = new object();
        private Librdkafka.LogDelegate logCallbackDelegate;
        private void LogCallback(IntPtr rk, SyslogLevel level, string fac, string buf)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            // Note: kafkaHandle can be null if the callback is during construction (in that case, we want the delegate to run).
            if (ownedKafkaHandle != null && ownedKafkaHandle.IsClosed) { return; }
            try
            {
                logHandler?.Invoke(new LogMessage(Util.Marshal.PtrToStringUTF8(Librdkafka.name(rk)), level, fac, buf));
            }
            catch (Exception)
            {
                // Eat any exception thrown by user log handler code.
            }
        }

        private Librdkafka.DeliveryReportDelegate DeliveryReportCallback;

        private void DeliveryReportCallbackImpl(IntPtr rk, IntPtr rkmessage, IntPtr opaque)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (ownedKafkaHandle.IsClosed) { return; }

            try
            {
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
            catch (Exception e)
            {
                handlerException = e;
            }
        }

        protected void ProduceImpl(
            string topic,
            ReadOnlySpan<byte> val,
            ReadOnlySpan<byte> key,
            Timestamp timestamp,
            Partition partition,
            IReadOnlyCollection<IHeader> headers,
            IDeliveryHandler deliveryHandler)
        {
            if (timestamp.Type != TimestampType.CreateTime)
            {
                if (timestamp != Timestamp.Default)
                {
                    throw new ArgumentException("Timestamp must be either Timestamp.Default, or Timestamp.CreateTime.");
                }
            }

            ErrorCode err;
            if (this.enableDeliveryReports && deliveryHandler != null)
            {
                // Passes the TaskCompletionSource to the delivery report callback via the msg_opaque pointer

                // Note: There is a level of indirection between the GCHandle and
                // physical memory address. GCHandle.ToIntPtr doesn't get the
                // physical address, it gets an id that refers to the object via
                // a handle-table.
                var gch = GCHandle.Alloc(deliveryHandler);
                var ptr = GCHandle.ToIntPtr(gch);

                err = KafkaHandle.Produce(
                    topic,
                    val,
                    key,
                    partition.Value,
                    timestamp.UnixTimestampMs,
                    headers,
                    ptr);

                if (err != ErrorCode.NoError)
                {
                    // note: freed in the delivery handler callback otherwise.
                    gch.Free();
                }
            }
            else
            {
                err = KafkaHandle.Produce(
                    topic,
                    val,
                    key,
                    partition.Value,
                    timestamp.UnixTimestampMs,
                    headers,
                    IntPtr.Zero);
            }

            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(KafkaHandle.CreatePossiblyFatalError(err, null));
            }
        }

        public int Poll(TimeSpan timeout)
        {
            if (manualPoll)
            {
                return this.KafkaHandle.Poll((IntPtr)timeout.TotalMillisecondsAsInt());
            }

            lock (pollSyncObj)
            {
                if (eventsServedCount == 0)
                {
                    Monitor.Wait(pollSyncObj, timeout);
                }

                var result = eventsServedCount;
                eventsServedCount = 0;
                return result;
            }
        }

        public int Flush(TimeSpan timeout)
        {
            var result = KafkaHandle.Flush(timeout.TotalMillisecondsAsInt());
            if (this.handlerException != null)
            {
                errorHandler?.Invoke(new Error(ErrorCode.Local_Application, handlerException.ToString()));
                var ex = this.handlerException;
                this.handlerException = null;
            }
            return result;
        }

        public void Flush(CancellationToken cancellationToken)
        {
            while (true)
            {
                int result = KafkaHandle.Flush(100);
                if (this.handlerException != null)
                {
                    errorHandler?.Invoke(new Error(ErrorCode.Local_Application, handlerException.ToString()));
                    var ex = this.handlerException;
                    this.handlerException = null;
                }

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

        
        /// <inheritdoc/>
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
                // Unpin partitioner functions
                foreach (var ph in this.partitionerHandles)
                {
                    ph.Free();
                }

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


        /// <inheritdoc/>
        public string Name
            => KafkaHandle.Name;


        /// <inheritdoc/>
        public int AddBrokers(string brokers)
            => KafkaHandle.AddBrokers(brokers);


        /// <inheritdoc/>
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

       
        protected ProducerBase(DependentProducerBuilder builder)
        {
            this.borrowedHandle = builder.Handle;

            if (!borrowedHandle.Owner.GetType().Name.Contains("Producer")) // much simpler than checking actual types.
            {
                throw new Exception("A Producer instance may only be constructed using the handle of another Producer instance.");
            }
        }

        protected ProducerBase()
        {
        }

        protected void Initialize(Config baseConfig)
        {
            var partitioners = baseConfig.partitioners;
            var defaultPartitioner = baseConfig.defaultPartitioner;

            // TODO: Make Tasks auto complete when EnableDeliveryReportsPropertyName is set to false.
            // TODO: Hijack the "delivery.report.only.error" configuration parameter and add functionality to enforce that Tasks 
            //       that never complete are never created when this is set to true.

            this.statisticsHandler = baseConfig.statisticsHandler;
            this.logHandler = baseConfig.logHandler;
            this.errorHandler = baseConfig.errorHandler;
            this.oAuthBearerTokenRefreshHandler = baseConfig.oAuthBearerTokenRefreshHandler;

            var config = Confluent.Kafka.Config.ExtractCancellationDelayMaxMs(baseConfig.config, out this.cancellationDelayMaxMs);

            this.DeliveryReportCallback = DeliveryReportCallbackImpl;

            Librdkafka.Initialize(null);

            var modifiedConfig = Library.NameAndVersionConfig
                .Concat(config
                    .Where(prop =>
                        prop.Key != ConfigPropertyNames.Producer.EnableBackgroundPoll &&
                        prop.Key != ConfigPropertyNames.Producer.EnableDeliveryReports &&
                        prop.Key != ConfigPropertyNames.Producer.DeliveryReportFields))
                .ToList();

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
                this.manualPoll = !bool.Parse(enableBackgroundPollObj);
            }

            var enableDeliveryReportsObj = config.FirstOrDefault(prop => prop.Key == ConfigPropertyNames.Producer.EnableDeliveryReports).Value;
            if (enableDeliveryReportsObj != null)
            {
                this.enableDeliveryReports = bool.Parse(enableDeliveryReportsObj);
            }

            var deliveryReportEnabledFieldsObj = config.FirstOrDefault(prop => prop.Key == ConfigPropertyNames.Producer.DeliveryReportFields).Value;
            if (deliveryReportEnabledFieldsObj != null)
            {
                var fields = deliveryReportEnabledFieldsObj.Replace(" ", "");
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
            IntPtr configPtr = configHandle.DangerousGetHandle();

            modifiedConfig.ForEach((kvp) =>
                {
                    if (kvp.Value == null) { throw new ArgumentNullException($"'{kvp.Key}' configuration parameter must not be null."); }
                    configHandle.Set(kvp.Key, kvp.Value);
                });

            if (enableDeliveryReports)
            {
                Librdkafka.conf_set_dr_msg_cb(configPtr, DeliveryReportCallback);
            }

            // Explicitly keep references to delegates so they are not reclaimed by the GC.
            errorCallbackDelegate = ErrorCallback;
            logCallbackDelegate = LogCallback;
            statisticsCallbackDelegate = StatisticsCallback;
            oAuthBearerTokenRefreshCallbackDelegate = OAuthBearerTokenRefreshCallback;

            if (errorHandler != null)
            {
                Librdkafka.conf_set_error_cb(configPtr, errorCallbackDelegate);
            }
            if (logHandler != null)
            {
                Librdkafka.conf_set_log_cb(configPtr, logCallbackDelegate);
            }
            if (statisticsHandler != null)
            {
                Librdkafka.conf_set_stats_cb(configPtr, statisticsCallbackDelegate);
            }
            if (oAuthBearerTokenRefreshHandler != null)
            {
                Librdkafka.conf_set_oauthbearer_token_refresh_cb(configPtr, oAuthBearerTokenRefreshCallbackDelegate);
            }

            Action<SafeTopicConfigHandle, PartitionerDelegate> addPartitionerToTopicConfig = (topicConfigHandle, partitioner) =>
            {
                Librdkafka.PartitionerDelegate librdkafkaPartitioner = (IntPtr rkt, IntPtr keydata, UIntPtr keylen, int partition_cnt, IntPtr rkt_opaque, IntPtr msg_opaque) =>
                    {
                        unsafe
                        {
                            var topicNamePtr = Librdkafka.topic_name(rkt);
                            var topic = Util.Marshal.PtrToStringUTF8(topicNamePtr);
                            var keyIsNull = keydata == IntPtr.Zero;
                            var keyBytes = keyIsNull
                                ? ReadOnlySpan<byte>.Empty
                                : new ReadOnlySpan<byte>(keydata.ToPointer(), (int)keylen);
                            return partitioner(topic, partition_cnt, keyBytes, keyIsNull);
                        }
                    };
                this.partitionerHandles.Add(GCHandle.Alloc(librdkafkaPartitioner));
                Librdkafka.topic_conf_set_partitioner_cb(topicConfigHandle.DangerousGetHandle(), librdkafkaPartitioner);
            };

            // Configure the default custom partitioner.
            if (defaultPartitioner != null)
            {
                // The default topic config may have been modified by topic-level
                // configuraton parameters passed down from the top level config.
                // If that's the case, duplicate the default topic config to avoid
                // colobbering any already configured values.
                var defaultTopicConfigHandle = configHandle.GetDefaultTopicConfig();
                SafeTopicConfigHandle topicConfigHandle =
                    defaultTopicConfigHandle.DangerousGetHandle() != IntPtr.Zero
                        ? defaultTopicConfigHandle.Duplicate()
                        : SafeTopicConfigHandle.Create();
                addPartitionerToTopicConfig(topicConfigHandle, defaultPartitioner);
                Librdkafka.conf_set_default_topic_conf(configPtr, topicConfigHandle.DangerousGetHandle());
            }

            this.ownedKafkaHandle = SafeKafkaHandle.Create(RdKafkaType.Producer, configPtr, this);
            configHandle.SetHandleAsInvalid();  // ownership was transferred.

            // Per-topic partitioners.
            foreach (var partitioner in partitioners)
            {
                var topicConfigHandle = this.ownedKafkaHandle.DuplicateDefaultTopicConfig();
                addPartitionerToTopicConfig(topicConfigHandle, partitioner.Value);
                this.ownedKafkaHandle.newTopic(partitioner.Key, topicConfigHandle.DangerousGetHandle());
            }

            if (!manualPoll)
            {
                callbackCts = new CancellationTokenSource();
                callbackTask = StartPollTask(callbackCts.Token);
            }
        }

        /// <inheritdoc/>
        public void InitTransactions(TimeSpan timeout)
            => KafkaHandle.InitTransactions(timeout.TotalMillisecondsAsInt());

        /// <inheritdoc/>
        public void BeginTransaction()
            => KafkaHandle.BeginTransaction();

        /// <inheritdoc/>
        public void CommitTransaction(TimeSpan timeout)
            => KafkaHandle.CommitTransaction(timeout.TotalMillisecondsAsInt());
        
        /// <inheritdoc/>
        public void CommitTransaction()
            => KafkaHandle.CommitTransaction(-1);

        /// <inheritdoc/>
        public void AbortTransaction(TimeSpan timeout)
            => KafkaHandle.AbortTransaction(timeout.TotalMillisecondsAsInt());

        /// <inheritdoc/>
        public void AbortTransaction()
            => KafkaHandle.AbortTransaction(-1);

        /// <inheritdoc/>
        public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout)
            => KafkaHandle.SendOffsetsToTransaction(offsets, groupMetadata, timeout.TotalMillisecondsAsInt());
    }
}
