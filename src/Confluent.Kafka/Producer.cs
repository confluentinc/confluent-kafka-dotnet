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


namespace Confluent.Kafka
{
    // TODO: provide method for dumping config?
    // TODO: allow custom partitioner to be set. refer to rdkafka-dotnet for a reference implmentation.

    public class Producer : IDisposable
    {
        private bool manualPoll;
        private bool disableDeliveryReports;

        internal const int RD_KAFKA_PARTITION_UA = -1;

        private IEnumerable<KeyValuePair<string, object>> topicConfig;

        private SafeDictionary<string, SafeTopicHandle> topicHandles
            = new SafeDictionary<string, SafeTopicHandle>();

        private SafeKafkaHandle kafkaHandle;

        private Task callbackTask;
        private CancellationTokenSource callbackCts;

        private const int POLL_TIMEOUT_MS = 100;
        private Task StartPollTask(CancellationToken ct)
            => Task.Factory.StartNew(() =>
                {
                    while (!ct.IsCancellationRequested)
                    {
                        this.kafkaHandle.Poll((IntPtr)POLL_TIMEOUT_MS);
                    }
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

        private SafeTopicHandle getKafkaTopicHandle(string topic)
        {
            // TODO: We should consider getting rid of these and add proper support in librdkafka itself
            //       (producev() with RD_KAFKA_V_TOPIC() is one step closer)
            if (topicHandles.ContainsKey(topic))
            {
                return topicHandles[topic];
            }

            var topicConfigHandle = SafeTopicConfigHandle.Create();
            if (topicConfig != null)
            {
                topicConfig
                    .ToList()
                    .ForEach((kvp) => { topicConfigHandle.Set(kvp.Key, kvp.Value.ToString()); });
            }
            topicConfigHandle.Set("produce.offset.report", "true");
            IntPtr configPtr = topicConfigHandle.DangerousGetHandle();

            // note: there is a possible (benign) race condition here - topicHandle could have already
            // been created for the topic (and possibly added to topicHandles). If the topicHandle has
            // already been created, rdkafka will return it and not create another. the call to rdkafka
            // is threadsafe.
            var topicHandle = kafkaHandle.Topic(topic, configPtr);
            topicHandles.Add(topic, topicHandle);

            return topicHandle;
        }

        private static readonly LibRdKafka.DeliveryReportDelegate DeliveryReportCallback = DeliveryReportCallbackImpl;

        /// <param name="opaque">
        ///     note: this property is set to that defined in rd_kafka_conf
        ///     (which is never used by confluent-kafka-dotnet).
        /// </param>
        private static void DeliveryReportCallbackImpl(IntPtr rk, IntPtr rkmessage, IntPtr opaque)
        {
            var msg = Marshal.PtrToStructure<rd_kafka_message>(rkmessage);

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
            long timestamp = LibRdKafka.message_timestamp(rkmessage, out timestampType) / 1000;
            var dateTime = new DateTime(0);
            if ((TimestampType)timestampType != TimestampType.NotAvailable)
            {
                dateTime = Timestamp.UnixTimestampMsToDateTime(timestamp);
            }

            deliveryHandler.HandleDeliveryReport(
                new Message (
                    // TODO: tracking handle -> topicName in addition to topicName -> handle could
                    //       avoid this marshalling / memory allocation cost.
                    Util.Marshal.PtrToStringUTF8(LibRdKafka.topic_name(msg.rkt)),
                    msg.partition,
                    msg.offset,
                    key,
                    val,
                    new Timestamp(dateTime, (TimestampType)timestampType),
                    msg.err
                )
            );
        }

        private sealed class TaskDeliveryHandler : TaskCompletionSource<Message>, IDeliveryHandler
        {
            public bool MarshalData { get { return true; } }

            public void HandleDeliveryReport(Message message)
            {
                SetResult(message);
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
            SafeTopicHandle topicHandle = getKafkaTopicHandle(topic);

            if (!this.disableDeliveryReports && deliveryHandler != null)
            {
                // Passes the TaskCompletionSource to the delivery report callback via the msg_opaque pointer
                var deliveryCompletionSource = deliveryHandler;
                var gch = GCHandle.Alloc(deliveryCompletionSource);
                var ptr = GCHandle.ToIntPtr(gch);

                if (topicHandle.Produce(val, valOffset, valLength, key, keyOffset, keyLength, partition, timestamp?.Ticks, ptr, blockIfQueueFull) != 0)
                {
                    var err = LibRdKafka.last_error();
                    gch.Free();
                    throw new KafkaException(err);
                }

                return;
            }

            if (topicHandle.Produce(val, valOffset, valLength, key, keyOffset, keyLength, partition, timestamp?.Ticks, IntPtr.Zero, blockIfQueueFull) != 0)
            {
                throw new KafkaException(LibRdKafka.last_error());
            }

            return;
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
        ///     librdkafka configuration parameters (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     TODO: Link to confluent-kafka-dotnet page with dotnet specific parameters also (i.e. default.topic.config).
        /// </param>
        /// <param name="manualPoll">
        ///     If true, does not start a dedicated polling thread to trigger events or receive delivery reports -
        ///     you must call the Poll method periodically instead.
        /// </param>
        /// <param name="disableDeliveryReports">
        ///     If true, disables notification of delivery reports. Note: if set to true and you use a ProduceAsync variant that return
        ///     a Task, the Tasks will never complete. Generally you should leave this parameter as false. Set it to true for "fire and
        ///     forget" semantics and a small boost in performance.
        /// </param>
        public Producer(IEnumerable<KeyValuePair<string, object>> config, bool manualPoll = false, bool disableDeliveryReports = false)
        {
            this.topicConfig = (IEnumerable<KeyValuePair<string, object>>)config.FirstOrDefault(prop => prop.Key == "default.topic.config").Value;
            this.manualPoll = manualPoll;
            this.disableDeliveryReports = disableDeliveryReports;

            var configHandle = SafeConfigHandle.Create();
            config
                .Where(prop => prop.Key != "default.topic.config")
                .ToList()
                .ForEach((kvp) => { configHandle.Set(kvp.Key, kvp.Value.ToString()); });

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

        // TODO: think about Poll and flushing.

        public void Poll(int millisecondsTimeout)
        {
            if (!manualPoll)
            {
                throw new InvalidOperationException("Poll method called when manualPoll not enabled.");
            }
            this.kafkaHandle.Poll((IntPtr)millisecondsTimeout);
        }

        public void Poll(TimeSpan timeout)
            => Poll(timeout.TotalMillisecondsAsInt());

        public void Poll()
            => Poll(-1);


        /// <summary>
        ///     Raised on critical errors, e.g. connection failures or all brokers down.
        /// </summary>
        public event EventHandler<Error> OnError;

        /// <summary>
        ///     Raised on librdkafka stats events.
        /// </summary>
        /// <remarks>
        ///     librdkafka statistics can be set using the statistics.interval.ms parameter.
        /// </remarks>
        public event EventHandler<string> OnStatistics;

        /// <summary>
        ///     Raised when there is information that should be to be logged.
        /// </summary>
        /// <remarks>
        ///     Specify which log level with the log_level configuration property.
        ///     can happen on any thread.
        /// </remarks>
        public event EventHandler<LogMessage> OnLog;

        public ISerializingProducer<TKey, TValue> GetSerializingProducer<TKey, TValue>(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
            => new SerializingProducer<TKey, TValue>(this, keySerializer, valueSerializer);


        public Task<Message> ProduceAsync(string topic, byte[] key, byte[] val)
            => Produce(topic, val, 0, val?.Length ?? 0, key, 0, key?.Length ?? 0, null, RD_KAFKA_PARTITION_UA, true);

        public Task<Message> ProduceAsync(string topic, byte[] key, byte[] val, DateTime timestamp)
            => Produce(topic, val, 0, val?.Length ?? 0, key, 0, key?.Length ?? 0, timestamp, RD_KAFKA_PARTITION_UA, true);


        public Task<Message> ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, null, RD_KAFKA_PARTITION_UA, true);

        public Task<Message> ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, DateTime timestamp)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, timestamp, RD_KAFKA_PARTITION_UA, true);

        public Task<Message> ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, DateTime timestamp, int partition)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, timestamp, partition, true);

        public Task<Message> ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, DateTime timestamp, int partition, bool blockIfQueueFull)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, timestamp, partition, blockIfQueueFull);

        public Task<Message> ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, int partition)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, null, partition, true);

        public Task<Message> ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, int partition, bool blockIfQueueFull)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, null, partition, blockIfQueueFull);

        public Task<Message> ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, bool blockIfQueueFull)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, null, RD_KAFKA_PARTITION_UA, blockIfQueueFull);


        public void ProduceAsync(string topic, byte[] key, byte[] val, IDeliveryHandler deliveryHandler)
            => Produce(topic, val, 0, val?.Length ?? 0, key, 0, key?.Length ?? 0, null, RD_KAFKA_PARTITION_UA, true, deliveryHandler);

        public void ProduceAsync(string topic, byte[] key, byte[] val, DateTime timestamp, IDeliveryHandler deliveryHandler)
            => Produce(topic, val, 0, val?.Length ?? 0, key, 0, key?.Length ?? 0, timestamp, RD_KAFKA_PARTITION_UA, true, deliveryHandler);


        public void ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, IDeliveryHandler deliveryHandler)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, null, RD_KAFKA_PARTITION_UA, true, deliveryHandler);

        public void ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, DateTime timestamp, IDeliveryHandler deliveryHandler)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, timestamp, RD_KAFKA_PARTITION_UA, true, deliveryHandler);

        public void ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, DateTime timestamp, int partition, IDeliveryHandler deliveryHandler)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, timestamp, partition, true, deliveryHandler);

        public void ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, DateTime timestamp, int partition, bool blockIfQueueFull, IDeliveryHandler deliveryHandler)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, timestamp, partition, blockIfQueueFull, deliveryHandler);

        public void ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, int partition, IDeliveryHandler deliveryHandler)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, null, partition, true, deliveryHandler);

        public void ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, int partition, bool blockIfQueueFull, IDeliveryHandler deliveryHandler)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, null, partition, blockIfQueueFull, deliveryHandler);

        public void ProduceAsync(string topic, byte[] key, int keyOffset, int keyLength, byte[] val, int valOffset, int valLength, bool blockIfQueueFull, IDeliveryHandler deliveryHandler)
            => Produce(topic, val, valOffset, valLength, key, keyOffset, keyLength, null, RD_KAFKA_PARTITION_UA, blockIfQueueFull, deliveryHandler);


        /// <summary>
        ///     Check if partition is available (has a leader broker).
        /// </summary>
        /// <returns>
        ///     Return true if the partition is available, else false.
        /// </returns>
        /// <remarks>
        ///     This function must only be called from inside a partitioner function.
        /// </remarks>
        public bool PartitionAvailable(string topic, int partition)
            => getKafkaTopicHandle(topic).PartitionAvailable(partition);

        public string Name
            => kafkaHandle.Name;

        /// <returns>
        ///     The current librdkafka out queue length.
        /// </returns>
        /// <remarks>
        ///     The out queue contains the messages and requests waiting to be sent,
        ///     or acknowledged by, the broker.
        /// </remarks>
        public long OutQueueLength
            => kafkaHandle.OutQueueLength;


        /// <summary>
        ///     Wait until all outstanding produce requests, et.al, are completed. This should typically be done prior
        ///     to destroying a producer instance to make sure all queued and in-flight produce requests are completed
        ///     before terminating. The wait is limited by the optional millisecondsTimeout parameter.
        /// </summary>
        /// <returns>
        ///     Returns the number of remaining messages and requests in queue.
        /// </returns>
        public long Flush(int millisecondsTimeout)
            => kafkaHandle.Flush(millisecondsTimeout);

        public long Flush()
            => kafkaHandle.Flush(-1);

        public void Dispose()
        {
            topicHandles.Dispose();

            if (!this.manualPoll)
            {
                // Note: It's necessary to wait on callbackTask before disposing kafkaHandle.
                // TODO: If called in a finalizer, can callbackTask be potentially null?
                callbackCts.Cancel();
                callbackTask.Wait();
            }
            kafkaHandle.Dispose();
        }

        // TODO: potentially use interface to avoid duplicate method docs.

        public List<GroupInfo> ListGroups(TimeSpan timeout)
            => kafkaHandle.ListGroups(timeout.TotalMillisecondsAsInt());

        public List<GroupInfo> ListGroups()
            => kafkaHandle.ListGroups(-1);


        public GroupInfo ListGroup(string group, TimeSpan timeout)
            => kafkaHandle.ListGroup(group, timeout.TotalMillisecondsAsInt());

        public GroupInfo ListGroup(string group)
            => kafkaHandle.ListGroup(group, -1);


        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
            => kafkaHandle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, timeout.TotalMillisecondsAsInt());

        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition)
            => kafkaHandle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, -1);


        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
            => kafkaHandle.GetWatermarkOffsets(topicPartition.Topic, topicPartition.Partition);


        private Metadata GetMetadata(bool allTopics, string topic, int millisecondsTimeout)
            => kafkaHandle.GetMetadata(allTopics, topic == null ? null : getKafkaTopicHandle(topic), millisecondsTimeout);

        public Metadata GetMetadata(bool allTopics, string topic, TimeSpan timeout)
            => GetMetadata(allTopics, topic, timeout.TotalMillisecondsAsInt());

        public Metadata GetMetadata(bool allTopics, string topic)
            => GetMetadata(allTopics, topic, -1);

        public Metadata GetMetadata()
            => GetMetadata(true, null, -1);
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

            // TODO: allow serializers to be set in the producer config IEnumerable<KeyValuePair<string, object>>.

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

            producer.OnLog += (sender, e) => OnLog?.Invoke(sender, e);
            producer.OnError += (sender, e) => OnError?.Invoke(sender, e);
            producer.OnStatistics += (sender, e) => OnStatistics?.Invoke(sender, e);
        }

        private class TypedTaskDeliveryHandlerShim : TaskCompletionSource<Message<TKey, TValue>>, IDeliveryHandler
        {
            public TypedTaskDeliveryHandlerShim(TKey key, TValue val)
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

                SetResult(mi);
            }
        }

        private Task<Message<TKey, TValue>> Produce(string topic, TKey key, TValue val, DateTime? timestamp, int partition, bool blockIfQueueFull)
        {
            var handler = new TypedTaskDeliveryHandlerShim(key, val);
            var keyBytes = KeySerializer?.Serialize(key);
            var valBytes = ValueSerializer?.Serialize(val);
            producer.Produce(topic, valBytes, 0, valBytes.Length, keyBytes, 0, keyBytes.Length, timestamp, partition, blockIfQueueFull, handler);
            return handler.Task;
        }

        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, DateTime timestamp, int partition, bool blockIfQueueFull)
            => Produce(topic, key, val, timestamp, partition, blockIfQueueFull);

        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, DateTime timestamp, int partition)
            => Produce(topic, key, val, timestamp, partition, true);

        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, DateTime timestamp)
            => Produce(topic, key, val, timestamp, Producer.RD_KAFKA_PARTITION_UA, true);

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

        private void Produce(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler, DateTime? timestamp, int partition, bool blockIfQueueFull)
        {
            var handler = new TypedDeliveryHandlerShim(key, val, deliveryHandler);
            var keyBytes = KeySerializer?.Serialize(key);
            var valBytes = ValueSerializer?.Serialize(val);
            producer.Produce(topic, valBytes, 0, valBytes.Length, keyBytes, 0, keyBytes.Length, timestamp, partition, blockIfQueueFull, handler);
        }

        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler, DateTime timestamp, int partition, bool blockIfQueueFull)
            => Produce(topic, key, val, deliveryHandler, timestamp, partition, blockIfQueueFull);

        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler, DateTime timestamp, int partition)
            => Produce(topic, key, val, deliveryHandler, timestamp, partition, true);

        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler, DateTime timestamp)
            => Produce(topic, key, val, deliveryHandler, timestamp, Producer.RD_KAFKA_PARTITION_UA, true);

        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => Produce(topic, key, val, deliveryHandler, null, Producer.RD_KAFKA_PARTITION_UA, true);

        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler, int partition, bool blockIfQueueFull)
            => Produce(topic, key, val, deliveryHandler, null, partition, blockIfQueueFull);

        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler, int partition)
            => Produce(topic, key, val, deliveryHandler, null, partition, true);

        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler, bool blockIfQueueFull)
            => Produce(topic, key, val, deliveryHandler, null, Producer.RD_KAFKA_PARTITION_UA, blockIfQueueFull);

        public string Name
            => producer.Name;

        public event EventHandler<LogMessage> OnLog;

        public event EventHandler<string> OnStatistics;

        public event EventHandler<Error> OnError;
    }


    public class Producer<TKey, TValue> : ISerializingProducer<TKey, TValue>, IDisposable
    {
        private readonly Producer producer;
        private readonly ISerializingProducer<TKey, TValue> serializingProducer;

        public Producer(
            IEnumerable<KeyValuePair<string, object>> config,
            ISerializer<TKey> keySerializer,
            ISerializer<TValue> valueSerializer,
            bool manualPoll = false, bool disableDeliveryReports = false)
        {
            producer = new Producer(config, manualPoll, disableDeliveryReports);
            serializingProducer = producer.GetSerializingProducer(keySerializer, valueSerializer);
            producer.OnLog += (sender, e) => OnLog?.Invoke(sender, e);
            producer.OnError += (sender, e) => OnError?.Invoke(sender, e);
            producer.OnStatistics += (sender, e) => OnStatistics?.Invoke(sender, e);
        }

        public ISerializer<TKey> KeySerializer
            => serializingProducer.KeySerializer;

        public ISerializer<TValue> ValueSerializer
            => serializingProducer.ValueSerializer;

        public string Name
            => serializingProducer.Name;

        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, DateTime timestamp, int partition, bool blockIfQueueFull)
            => serializingProducer.ProduceAsync(topic, key, val, timestamp, partition, blockIfQueueFull);

        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, DateTime timestamp, int partition)
            => serializingProducer.ProduceAsync(topic, key, val, timestamp, partition);

        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, DateTime timestamp)
            => serializingProducer.ProduceAsync(topic, key, val, timestamp);

        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val)
            => serializingProducer.ProduceAsync(topic, key, val);

        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, int partition, bool blockIfQueueFull)
            => serializingProducer.ProduceAsync(topic, key, val, partition, blockIfQueueFull);

        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, int partition)
            => serializingProducer.ProduceAsync(topic, key, val, partition);

        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, bool blockIfQueueFull)
            => serializingProducer.ProduceAsync(topic, key, val, blockIfQueueFull);


        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler, DateTime timestamp, int partition, bool blockIfQueueFull)
            => ProduceAsync(topic, key, val, deliveryHandler, timestamp, partition, blockIfQueueFull);

        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler, DateTime timestamp, int partition)
            => ProduceAsync(topic, key, val, deliveryHandler, timestamp, partition);

        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler, DateTime timestamp)
            => ProduceAsync(topic, key, val, deliveryHandler, timestamp);

        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => ProduceAsync(topic, key, val, deliveryHandler);

        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler, int partition, bool blockIfQueueFull)
            => ProduceAsync(topic, key, val, deliveryHandler, partition, blockIfQueueFull);

        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler, int partition)
            => ProduceAsync(topic, key, val, deliveryHandler, partition);

        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler, bool blockIfQueueFull)
            => ProduceAsync(topic, key, val, deliveryHandler, blockIfQueueFull);


        public event EventHandler<LogMessage> OnLog;

        public event EventHandler<string> OnStatistics;

        public event EventHandler<Error> OnError;


        public long Flush(int timeout)
            => producer.Flush(timeout);

        public long Flush()
            => producer.Flush();


        public void Dispose()
            => producer.Dispose();


        public List<GroupInfo> ListGroups(TimeSpan timeout)
            => producer.ListGroups(timeout);

        public List<GroupInfo> ListGroups()
            => producer.ListGroups();


        public GroupInfo ListGroup(string group, TimeSpan timeout)
            => producer.ListGroup(group, timeout);

        public GroupInfo ListGroup(string group)
            => producer.ListGroup(group);


        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
            => producer.QueryWatermarkOffsets(topicPartition, timeout);

        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition)
            => producer.QueryWatermarkOffsets(topicPartition);


        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
            => producer.GetWatermarkOffsets(topicPartition);


        /// <summary>
        ///     - allTopics=true - request all topics from cluster
        ///     - allTopics=false, topic=null - request only locally known topics (topic_new():ed topics or otherwise locally referenced once, such as consumed topics)
        ///     - allTopics=false, topic=valid - request specific topic
        /// </summary>
        public Metadata GetMetadata(bool allTopics, string topic, TimeSpan timeout)
            => producer.GetMetadata(allTopics, topic, timeout);

        public Metadata GetMetadata(bool allTopics, string topic)
            => producer.GetMetadata(allTopics, topic);
    }
}
