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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;


namespace Confluent.Kafka
{
    /// <summary>
    ///     A high-level Apache Kafka producer, excluding
    ///     any public methods for producing messages.
    /// </summary>
    public class ProducerBase : IProducerBase
    {
        private int cancellationDelayMaxMs;
        private bool disposeHasBeenCalled = false;
        private object disposeHasBeenCalledLockObj = new object();

        private readonly bool manualPoll = false;
        internal readonly bool enableDeliveryReports = true;
        internal readonly bool enableDeliveryReportKey = true;
        internal readonly bool enableDeliveryReportValue = true;
        internal readonly bool enableDeliveryReportTimestamp = true;
        internal readonly bool enableDeliveryReportHeaders = true;
        internal readonly bool enableDeliveryReportPersistedStatus = true;

        private readonly SafeKafkaHandle ownedKafkaHandle;
        private readonly Handle borrowedHandle;

        internal SafeKafkaHandle KafkaHandle
            => ownedKafkaHandle != null 
                ? ownedKafkaHandle
                : borrowedHandle.LibrdkafkaHandle;

        private readonly Task callbackTask;
        private readonly CancellationTokenSource callbackCts;

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


        private readonly Librdkafka.ErrorDelegate errorCallbackDelegate;
        private void ErrorCallback(IntPtr rk, ErrorCode err, string reason, IntPtr opaque)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (ownedKafkaHandle.IsClosed) { return; }
            onError?.Invoke(this, KafkaHandle.CreatePossiblyFatalError(err, reason));
        }


        private readonly Librdkafka.StatsDelegate statsCallbackDelegate;
        private int StatsCallback(IntPtr rk, IntPtr json, UIntPtr json_len, IntPtr opaque)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (ownedKafkaHandle.IsClosed) { return 0; }
            onStatistics?.Invoke(this, Util.Marshal.PtrToStringUTF8(json));
            return 0; // instruct librdkafka to immediately free the json ptr.
        }


        private object loggerLockObj = new object();
        private readonly Librdkafka.LogDelegate logCallbackDelegate;
        private void LogCallback(IntPtr rk, SyslogLevel level, string fac, string buf)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            // Note: kafkaHandle can be null if the callback is during construction (in that case, we want the delegate to run).
            if (ownedKafkaHandle != null && ownedKafkaHandle.IsClosed) { return; }
            onLog?.Invoke(this, new LogMessage(Util.Marshal.PtrToStringUTF8(Librdkafka.name(rk)), level, fac, buf));
        }

        private Librdkafka.DeliveryReportDelegate DeliveryReportCallback;

        private void DeliveryReportCallbackImpl(IntPtr rk, IntPtr rkmessage, IntPtr opaque)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (ownedKafkaHandle.IsClosed) { return; }

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
                new DeliveryReport 
                {
                    // Topic is not set here in order to avoid the marshalling cost.
                    // Instead, the delivery handler is expected to cache the topic string.
                    Partition = msg.partition, 
                    Offset = msg.offset, 
                    Error = KafkaHandle.CreatePossiblyFatalError(msg.err, null),
                    Message = new Message { Timestamp = new Timestamp(timestamp, (TimestampType)timestampType), Headers = headers }
                }
            );
        }

        /// <summary>
        ///     Produce a message.
        /// </summary>
        /// <param name="topic">
        ///     The topic to produce to.
        /// </param>
        /// <param name="val">
        ///     A byte array containing the message value,
        ///     or null.
        /// </param>
        /// <param name="valOffset">
        ///     Offset into the val byte array that the value
        ///     data starts (ignored if null).
        /// </param>
        /// <param name="valLength">
        ///     Length of the message value data.
        /// </param>
        /// <param name="key">
        ///     A byte arry containing the message key,
        ///     or null.
        /// </param>
        /// <param name="keyOffset">
        ///     Offset into the key byte array that the key
        ///     data starts (ignored if null).
        /// </param>
        /// <param name="keyLength">
        ///     Lenght of the message key data.
        /// </param>
        /// <param name="timestamp">
        ///     Message timestamp.
        /// </param>
        /// <param name="partition">
        ///     The specific partition to produce to or Partition.Any
        ///     if this should be determined by a partitioner.
        /// </param>
        /// <param name="headers">
        ///     Message headers.
        /// </param>
        /// <param name="deliveryHandler">
        ///     Handler to invoke when delivery report is available.
        /// </param>
        protected void Produce(
            string topic,
            byte[] val, int valOffset, int valLength,
            byte[] key, int keyOffset, int keyLength,
            Timestamp timestamp,
            Partition partition, 
            IEnumerable<Header> headers,
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

        private event EventHandler<LogMessage> onLog;

        /// <summary>
        ///     Refer to <see cref="IClient.OnLog" />.
        /// </summary>
        public event EventHandler<LogMessage> OnLog
        {
            add
            {
                if (ownedKafkaHandle != null) { onLog += value; }
                else { borrowedHandle.Owner.OnLog += value; }
            }
            remove
            {
                if (ownedKafkaHandle != null) { onLog -= value; }
                else { borrowedHandle.Owner.OnLog -= value; }
            }
        }


        private event EventHandler<Error> onError;

        /// <summary>
        ///     Refer to <see cref="IClient.OnError" />.
        /// </summary>
        public event EventHandler<Error> OnError
        {
            add
            {
                if (ownedKafkaHandle != null) { onError += value; }
                else { borrowedHandle.Owner.OnError += value; }
            }
            remove
            {
                if (ownedKafkaHandle != null) { onError -= value; }
                else { borrowedHandle.Owner.OnError -= value; }
            }
        }

        private event EventHandler<string> onStatistics;

        /// <summary>
        ///     Refer to <see cref="IClient.OnStatistics" />.
        /// </summary>
        public event EventHandler<string> OnStatistics
        {
            add
            {
                if (ownedKafkaHandle != null) { onStatistics += value; }
                else { borrowedHandle.Owner.OnStatistics += value; }
            }
            remove
            {
                if (ownedKafkaHandle != null) { onStatistics -= value; }
                else { borrowedHandle.Owner.OnStatistics -= value; }
            }
        }

        /// <summary>
        ///     Creates a new <see cref="ProducerBase" /> instance.
        /// </summary>
        /// <param name="handle">
        ///     An existing librdkafka producer handle to use for 
        ///     communications with Kafka brokers.
        /// </param>
        public ProducerBase(Handle handle)
        {
            this.borrowedHandle = handle;
        }

        /// <summary>
        ///     Creates a new <see cref="ProducerBase" /> instance.
        /// </summary>
        /// <param name="config">
        ///     A collection of librdkafka configuration parameters 
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     and parameters specific to this client (refer to: 
        ///     <see cref="ConfigPropertyNames" />).
        ///     At a minimum, 'bootstrap.servers' must be specified.
        /// </param>
        public ProducerBase(IEnumerable<KeyValuePair<string, string>> config)
        {
            // TODO: Make Tasks auto complete when EnableDeliveryReportsPropertyName is set to false.
            // TODO: Hijack the "delivery.report.only.error" configuration parameter and add functionality to enforce that Tasks 
            //       that never complete are never created when this is set to true.

            config = Config.GetCancellationDelayMaxMs(config, out this.cancellationDelayMaxMs);

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
                if (kvp.Value == null) throw new ArgumentException($"'{kvp.Key}' configuration parameter must not be null.");
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
            statsCallbackDelegate = StatsCallback;

            // TODO: provide some mechanism whereby calls to the error and log callbacks are cached until
            //       such time as event handlers have had a chance to be registered.
            Librdkafka.conf_set_error_cb(configPtr, errorCallbackDelegate);
            Librdkafka.conf_set_log_cb(configPtr, logCallbackDelegate);
            Librdkafka.conf_set_stats_cb(configPtr, statsCallbackDelegate);

            this.ownedKafkaHandle = SafeKafkaHandle.Create(RdKafkaType.Producer, configPtr, this);
            configHandle.SetHandleAsInvalid(); // config object is no longer useable.

            if (!manualPoll)
            {
                callbackCts = new CancellationTokenSource();
                callbackTask = StartPollTask(callbackCts.Token);
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
        ///     Releases all resources used by this <see cref="Producer" />.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        /// <summary>
        ///     Releases the unmanaged resources used by the
        ///     <see cref="ProducerBase" />
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

    }
}
