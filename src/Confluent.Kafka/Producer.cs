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

        const int RD_KAFKA_PARTITION_UA = -1;

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

        private void ErrorCallback(IntPtr rk, ErrorCode err, string reason, IntPtr opaque)
        {
            OnError?.Invoke(this, new Error(err, reason));
        }

        private int StatsCallback(IntPtr rk, IntPtr json, UIntPtr json_len, IntPtr opaque)
        {
            OnStatistics?.Invoke(this, Util.Marshal.PtrToStringUTF8(json));
            return 0; // instruct librdkafka to immediately free the json ptr.
        }

        private void LogCallback(IntPtr rk, int level, string fac, string buf)
        {
            var name = Util.Marshal.PtrToStringUTF8(LibRdKafka.name(rk));

            if (OnLog == null)
            {
                // Log to stderr by default if no logger is specified.
                Loggers.ConsoleLogger(this, new LogMessage(name, level, fac, buf ));
                return;
            }

            OnLog.Invoke(this, new LogMessage(name, level, fac, buf ));
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
            IntPtr configPtr = topicConfigHandle.Dup();
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
                // TODO: is timestamp guarenteed to be in valid range if type == NotAvailable? if so, remove this conditional.
                dateTime = Timestamp.UnixTimestampMsToDateTime(timestamp);
            }

            deliveryHandler.HandleDeliveryReport(
                new MessageInfo (
                    // TODO: tracking handle -> topicName in addition to topicName -> handle could
                    //       avoid this marshalling / memory allocation cost.
                    Util.Marshal.PtrToStringUTF8(LibRdKafka.topic_name(msg.rkt)),
                    msg.partition,
                    msg.offset,
                    key,
                    val,
                    new Timestamp(dateTime, (TimestampType)timestampType),
                    new Error(
                        msg.err,
                        msg.err == ErrorCode.NO_ERROR
                            ? null
                            : Util.Marshal.PtrToStringUTF8(LibRdKafka.err2str(msg.err))
                    )
                )
            );
        }

        private sealed class TaskDeliveryHandler : TaskCompletionSource<MessageInfo>, IDeliveryHandler
        {
            public bool MarshalData { get { return true; } }

            public void HandleDeliveryReport(MessageInfo messageInfo)
            {
                SetResult(messageInfo);
            }
        }

        private void Produce(
            string topic,
            byte[] val, int valOffset, int valLength,
            byte[] key, int keyOffset, int keyLength,
            long? timestamp,
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

                if (topicHandle.Produce(val, valOffset, valLength, key, keyOffset, keyLength, partition, timestamp, ptr, blockIfQueueFull) != 0)
                {
                    var err = LibRdKafka.last_error();
                    gch.Free();
                    throw RdKafkaException.FromErr(err, "Could not produce message");
                }

                return;
            }

            if (topicHandle.Produce(val, valOffset, valLength, key, keyOffset, keyLength, partition, timestamp, IntPtr.Zero, blockIfQueueFull) != 0)
            {
                var err = LibRdKafka.last_error();
                throw RdKafkaException.FromErr(err, "Could not produce message");
            }

            return;
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
            // TODO: What's dup do exactly? do we need it?
            IntPtr rdKafkaConfigPtr = configHandle.Dup();

            if (!disableDeliveryReports)
            {
                LibRdKafka.conf_set_dr_msg_cb(rdKafkaConfigPtr, DeliveryReportCallback);
            }

            // TODO: provide some mechanism whereby calls to the error and log callbacks are cached until
            //       such time as event handlers have had a chance to be registered.
            LibRdKafka.conf_set_error_cb(rdKafkaConfigPtr, ErrorCallback);
            LibRdKafka.conf_set_log_cb(rdKafkaConfigPtr, LogCallback);
            LibRdKafka.conf_set_stats_cb(rdKafkaConfigPtr, StatsCallback);

            this.kafkaHandle = SafeKafkaHandle.Create(RdKafkaType.Producer, rdKafkaConfigPtr);

            if (!manualPoll)
            {
                callbackCts = new CancellationTokenSource();
                callbackTask = StartPollTask(callbackCts.Token);
            }
        }

        // TODO: think about this and flushing.
        public void Poll(TimeSpan timeout)
        {
            if (!manualPoll)
            {
                throw new InvalidOperationException("Poll method called when manualPoll not enabled.");
            }
            this.kafkaHandle.Poll((IntPtr)((int)timeout.TotalMilliseconds));
        }

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

        public Task<MessageInfo> ProduceAsync(string topic, byte[] key, byte[] val, DateTime? timestamp = null, int? partition = null, bool blockIfQueueFull = true)
        {
            var deliveryCompletionSource = new TaskDeliveryHandler();
            Produce(topic, val, 0, val?.Length ?? 0, key, 0, key?.Length ?? 0, timestamp?.Ticks, partition ?? RD_KAFKA_PARTITION_UA, blockIfQueueFull, deliveryCompletionSource);
            return deliveryCompletionSource.Task;
        }

        public Task<MessageInfo> ProduceAsync(string topic, ArraySegment<byte>? key, ArraySegment<byte>? val, DateTime? timestamp = null, int? partition = null, bool blockIfQueueFull = true)
        {
            var deliveryCompletionSource = new TaskDeliveryHandler();
            Produce(topic, val == null ? null : val.Value.Array, val == null ? 0 : val.Value.Offset, val == null ? 0 : val.Value.Count, key == null ? null : key.Value.Array, key == null ? 0 : key.Value.Offset, key == null ? 0 : key.Value.Count, timestamp?.Ticks, partition ?? RD_KAFKA_PARTITION_UA, blockIfQueueFull, deliveryCompletionSource);
            return deliveryCompletionSource.Task;
        }

        public void ProduceAsync(string topic, byte[] key, byte[] val, IDeliveryHandler deliveryHandler, DateTime? timestamp = null, int? partition = null, bool blockIfQueueFull = true)
            => Produce(topic, val, 0, val?.Length ?? 0, key, 0, key?.Length ?? 0, timestamp?.Ticks, partition ?? RD_KAFKA_PARTITION_UA, blockIfQueueFull, deliveryHandler);

        public void ProduceAsync(string topic, ArraySegment<byte>? key, ArraySegment<byte>? val, IDeliveryHandler deliveryHandler, DateTime? timestamp = null, int? partition = null, bool blockIfQueueFull = true)
            => Produce(
                topic,
                val == null ? null : val.Value.Array,
                val == null ? 0 : val.Value.Offset,
                val == null ? 0 : val.Value.Count,
                key == null ? null : key.Value.Array,
                key == null ? 0 : key.Value.Offset,
                key == null ? 0 : key.Value.Count,
                timestamp?.Ticks,
                partition ?? RD_KAFKA_PARTITION_UA,
                blockIfQueueFull,
                deliveryHandler);

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

        /// <summary>
        ///     The number of messages and requests waiting to be sent to,
        ///     or acknowledged by the broker.
        /// </summary>
        public long OutQueueLength
            => kafkaHandle.OutQueueLength;

        public void Flush()
        {
            kafkaHandle.Flush(-1);
        }

        /// <summary>
        ///     Wait until all outstanding produce requests, et.al, are completed. This should typically be done prior
        //      to destroying a producer instance to make sure all queued and in-flight produce requests are completed
        //      before terminating.
        /// </summary>
        public void Flush(TimeSpan timeout)
        {
            int timeoutMs;
            checked { timeoutMs = (int)timeout.TotalMilliseconds; }
            kafkaHandle.Flush(timeoutMs);
        }

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

        public List<GroupInfo> ListGroups(TimeSpan? timeout = null)
            => kafkaHandle.ListGroups(timeout);

        public GroupInfo ListGroup(string group, TimeSpan? timeout = null)
            => kafkaHandle.ListGroup(group, timeout);

        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan? timeout = null)
            => kafkaHandle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, timeout);

        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
            => kafkaHandle.GetWatermarkOffsets(topicPartition.Topic, topicPartition.Partition);

        public Metadata GetMetadata(bool allTopics = true, string topic = null, TimeSpan? timeout = null)
            => kafkaHandle.GetMetadata(allTopics, topic == null ? null : getKafkaTopicHandle(topic), timeout);
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

        private class TypedTaskDeliveryHandlerShim : TaskCompletionSource<MessageInfo<TKey, TValue>>, IDeliveryHandler
        {
            public TypedTaskDeliveryHandlerShim(TKey key, TValue val)
            {
                Key = key;
                Value = val;
            }

            public TKey Key;

            public TValue Value;

            public bool MarshalData { get { return false; } }

            public void HandleDeliveryReport(MessageInfo messageInfo)
            {
                var mi = new MessageInfo<TKey, TValue>(
                    messageInfo.Topic,
                    messageInfo.Partition,
                    messageInfo.Offset,
                    Key,
                    Value,
                    messageInfo.Timestamp,
                    messageInfo.Error
                );

                SetResult(mi);
            }
        }

        public Task<MessageInfo<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, DateTime? timestamp = null, int? partition = null, bool blockIfQueueFull = true)
        {
            var handler = new TypedTaskDeliveryHandlerShim(key, val);
            producer.ProduceAsync(topic, KeySerializer?.Serialize(key), ValueSerializer?.Serialize(val), handler, timestamp, partition, blockIfQueueFull);
            return handler.Task;
        }

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

            public void HandleDeliveryReport(MessageInfo messageInfo)
            {
                Handler.HandleDeliveryReport(new MessageInfo<TKey, TValue>(
                    messageInfo.Topic,
                    messageInfo.Partition,
                    messageInfo.Offset,
                    Key,
                    Value,
                    messageInfo.Timestamp,
                    messageInfo.Error
                ));
            }
        }

        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler, DateTime? timestamp = null, int? partition = null, bool blockIfQueueFull = true)
        {
            var handler = new TypedDeliveryHandlerShim(key, val, deliveryHandler);
            producer.ProduceAsync(topic, KeySerializer?.Serialize(key), ValueSerializer?.Serialize(val), handler, timestamp, partition, blockIfQueueFull);
        }

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

        public Task<MessageInfo<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, DateTime? timestamp = null, int? partition = null, bool blockIfQueueFull = true)
            => serializingProducer.ProduceAsync(topic, key, val, timestamp, partition, blockIfQueueFull);

        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler, DateTime? timestamp = null, int? partition = null, bool blockIfQueueFull = true)
            => serializingProducer.ProduceAsync(topic, key, val, deliveryHandler, timestamp, partition, blockIfQueueFull);


        public event EventHandler<LogMessage> OnLog;

        public event EventHandler<string> OnStatistics;

        public event EventHandler<Error> OnError;

        public void Flush()
            => producer.Flush();

        public void Flush(TimeSpan timeout)
            => producer.Flush(timeout);

        public void Dispose()
            => producer.Dispose();


        public List<GroupInfo> ListGroups(TimeSpan? timeout = null)
            => producer.ListGroups(timeout);

        public GroupInfo ListGroup(string group, TimeSpan? timeout = null)
            => producer.ListGroup(group, timeout);

        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan? timeout = null)
            => producer.QueryWatermarkOffsets(topicPartition, timeout);

        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
            => producer.GetWatermarkOffsets(topicPartition);

        /// <summary>
        ///     - allTopics=true - request all topics from cluster
        ///     - allTopics=false, topic=null - request only locally known topics (topic_new():ed topics or otherwise locally referenced once, such as consumed topics)
        ///     - allTopics=false, topic=valid - request specific topic
        /// </summary>
        public Metadata GetMetadata(bool allTopics = true, string topic = null, TimeSpan? timeout = null)
            => producer.GetMetadata(allTopics, topic, timeout);
    }
}
