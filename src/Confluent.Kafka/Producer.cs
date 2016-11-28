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

        private const int RD_KAFKA_PARTITION_UA = -1;

        private IEnumerable<KeyValuePair<string, object>> topicConfig;

        private SafeDictionary<string, SafeTopicHandle> topicHandles = new SafeDictionary<string, SafeTopicHandle>();

        private SafeKafkaHandle kafkaHandle;

        private Task callbackTask;
        private CancellationTokenSource callbackCts;

        private const int POLL_TIMEOUT_MS = 1000;
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
            OnError?.Invoke(this, new ErrorArgs() { ErrorCode = err, Reason = reason });
        }

        private int StatsCallback(IntPtr rk, IntPtr json, UIntPtr json_len, IntPtr opaque)
        {
            OnStatistics?.Invoke(this, Util.Marshal.PtrToStringUTF8(json));
            return 0; // TODO: What's this for?
        }

        private void LogCallback(IntPtr rk, int level, string fac, string buf)
        {
            var name = Util.Marshal.PtrToStringUTF8(LibRdKafka.name(rk));

            if (OnLog == null)
            {
                // Log to stderr by default if no logger is specified.
                Loggers.ConsoleLogger(this, new LogArgs() { Name = name, Level = level, Facility = fac, Message = buf });
                return;
            }

            OnLog.Invoke(this, new LogArgs() { Name = name, Level = level, Facility = fac, Message = buf });
        }

        private SafeTopicHandle getKafkaTopicHandle(string topic)
        {
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

        private static void DeliveryReportCallbackImpl(IntPtr rk, ref rd_kafka_message rkmessage, IntPtr opaque)
        {
            // msg_opaque was set by Topic.Produce to be an IDeliveryHandler.
            // TODO: why rkmessage._private and not opaque here?

            if (rkmessage._private == IntPtr.Zero)
            {
                // Note: this can occur if the ProduceAsync overload that accepts a DeliveryHandler
                // was used and the delivery handler was set to null.
                return;
            }

            var gch = GCHandle.FromIntPtr(rkmessage._private);
            var deliveryHandler = (IDeliveryHandler) gch.Target;
            gch.Free();

            byte[] key = null;
            byte[] val = null;
            if (deliveryHandler.MarshalData)
            {
                if (rkmessage.key != IntPtr.Zero)
                {
                    key = new byte[(int)rkmessage.key_len];
                    System.Runtime.InteropServices.Marshal.Copy(rkmessage.key, key, 0, (int)rkmessage.key_len);
                }
                if (rkmessage.val != IntPtr.Zero)
                {
                    val = new byte[(int)rkmessage.len];
                    System.Runtime.InteropServices.Marshal.Copy(rkmessage.val, val, 0, (int)rkmessage.len);
                }
            }

            deliveryHandler.SetDeliveryReport(
                new MessageInfo
                {
                    // TODO: tracking handle -> topicName in addition to topicName -> handle could
                    //       avoid this marshalling / memory allocation cost.
                    Topic = Util.Marshal.PtrToStringUTF8(LibRdKafka.topic_name(rkmessage.rkt)),
                    Offset = rkmessage.offset,
                    Partition = rkmessage.partition,
                    ErrorCode = rkmessage.err,
                    ErrorMessage = rkmessage.err == ErrorCode.NO_ERROR
                        ? null
                        : Util.Marshal.PtrToStringUTF8(LibRdKafka.err2str(rkmessage.err)),
                    Key = key,
                    Value = val
                }
            );
        }

        private sealed class TaskDeliveryHandler : TaskCompletionSource<MessageInfo>, IDeliveryHandler
        {
            public bool MarshalData { get { return true; } }

            public void SetDeliveryReport(MessageInfo messageInfo)
            {
                if (messageInfo.ErrorCode == ErrorCode.NO_ERROR)
                {
                    SetResult(messageInfo);
                }
                else
                {
                    SetException(new DeliveryException(messageInfo));
                }
            }
        }

        private void Produce(
            string topic,
            byte[] val, int valOffset, int valLength,
            byte[] key, int keyOffset, int keyLength,
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

                if (topicHandle.Produce(val, valOffset, valLength, key, keyOffset, keyLength, partition, ptr, blockIfQueueFull) != 0)
                {
                    var err = LibRdKafka.last_error();
                    gch.Free();
                    throw RdKafkaException.FromErr(err, "Could not produce message");
                }

                return;
            }

            if (topicHandle.Produce(val, valOffset, valLength, key, keyOffset, keyLength, partition, IntPtr.Zero, blockIfQueueFull) != 0)
            {
                var err = LibRdKafka.last_error();
                throw RdKafkaException.FromErr(err, "Could not produce message");
            }

            return;
        }

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
        public event EventHandler<ErrorArgs> OnError;

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
        public event EventHandler<LogArgs> OnLog;

        public ISerializingProducer<TKey, TValue> Wrap<TKey, TValue>(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
        {
            return new SerializingProducer<TKey, TValue>(this, keySerializer, valueSerializer);
        }

        public Task<MessageInfo> ProduceAsync(string topic, byte[] key, byte[] val, int? partition = null, bool blockIfQueueFull = true)
        {
            var deliveryCompletionSource = new TaskDeliveryHandler();
            Produce(topic, val, 0, val?.Length ?? 0, key, 0, key?.Length ?? 0, partition ?? RD_KAFKA_PARTITION_UA, blockIfQueueFull, deliveryCompletionSource);
            return deliveryCompletionSource.Task;
        }

        public Task<MessageInfo> ProduceAsync(string topic, ArraySegment<byte> key, ArraySegment<byte> val, int? partition = null, bool blockIfQueueFull = true)
        {
            var deliveryCompletionSource = new TaskDeliveryHandler();
            Produce(topic, val.Array, val.Offset, val.Count, key.Array, key.Offset, key.Count, partition ?? RD_KAFKA_PARTITION_UA, blockIfQueueFull, deliveryCompletionSource);
            return deliveryCompletionSource.Task;
        }

        public void ProduceAsync(string topic, byte[] key, byte[] val, IDeliveryHandler deliveryHandler, int? partition = null, bool blockIfQueueFull = true)
        {
            Produce(topic, val, 0, val?.Length ?? 0, key, 0, key?.Length ?? 0, partition ?? RD_KAFKA_PARTITION_UA, blockIfQueueFull, deliveryHandler);
        }

        public void ProduceAsync(string topic, ArraySegment<byte> key, ArraySegment<byte> val, IDeliveryHandler deliveryHandler, int? partition = null, bool blockIfQueueFull = true)
        {
            Produce(topic, val.Array, val.Offset, val.Count, key.Array, key.Offset, key.Count, partition ?? RD_KAFKA_PARTITION_UA, blockIfQueueFull, deliveryHandler);
        }



        /// <summary>
        ///     Check if partition is available (has a leader broker).
        /// </summary>
        /// <returns>
        ///     Return true if the partition is available, else false.
        /// </returns>
        /// <remarks>
        ///     This function must only be called from inside a partitioner function.
        /// </remarks>
        public bool PartitionAvailable(string topic, int partition) => getKafkaTopicHandle(topic).PartitionAvailable(partition);

        public string Name => kafkaHandle.Name;

        /// <summary>
        ///     The number of messages and requests waiting to be sent to,
        ///     or acknowledged by the broker.
        /// </summary>
        public long OutQueueLength => kafkaHandle.OutQueueLength;


        // TODO: Question - does librdkafka make sure these messages are removed after a timeout (even on failure)
        public void Flush(int timeoutMilliseconds = int.MaxValue) => Flush(TimeSpan.FromMilliseconds(timeoutMilliseconds));

        /// <summary>
        ///     Wait until all outstanding produce requests, et.al, are completed. This should typically be done prior
        //      to destroying a producer instance to make sure all queued and in-flight produce requests are completed
        //      before terminating.
        /// </summary>
        public void Flush(TimeSpan timeout)
        {
            kafkaHandle.Flush(timeout);
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

        public Offsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan? timeout = null)
            => kafkaHandle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, timeout);

        public Offsets GetWatermarkOffsets(TopicPartition topicPartition)
            => kafkaHandle.GetWatermarkOffsets(topicPartition.Topic, topicPartition.Partition);

        public Metadata GetMetadata(string topic = null, bool includeInternal = false, TimeSpan? timeout = null)
            => kafkaHandle.GetMetadata(topic == null ? null : getKafkaTopicHandle(topic), includeInternal, timeout);
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

            public void SetDeliveryReport(MessageInfo messageInfo)
            {
                var mi = new MessageInfo<TKey, TValue>()
                {
                    Topic = messageInfo.Topic,
                    Partition = messageInfo.Partition,
                    Offset = messageInfo.Offset,
                    ErrorCode = messageInfo.ErrorCode,
                    ErrorMessage = messageInfo.ErrorMessage,
                    Timestamp = messageInfo.Timestamp,
                    Key = Key,
                    Value = Value
                };

                if (messageInfo.ErrorCode == ErrorCode.NO_ERROR)
                {
                    SetResult(mi);
                }
                else
                {
                    SetException(new DeliveryException<TKey, TValue>(mi));
                }
            }
        }

        public Task<MessageInfo<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, int? partition = null, bool blockIfQueueFull = true)
        {
            var handler = new TypedTaskDeliveryHandlerShim(key, val);
            producer.ProduceAsync(topic, KeySerializer?.Serialize(key), ValueSerializer?.Serialize(val), handler, partition, blockIfQueueFull);
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

            public void SetDeliveryReport(MessageInfo messageInfo)
            {
                Handler.SetDeliveryReport(new MessageInfo<TKey, TValue>()
                    {
                        Topic = messageInfo.Topic,
                        Partition = messageInfo.Partition,
                        Offset = messageInfo.Offset,
                        ErrorCode = messageInfo.ErrorCode,
                        ErrorMessage = messageInfo.ErrorMessage,
                        Timestamp = messageInfo.Timestamp,
                        Key = Key,
                        Value = Value
                    });
            }
        }

        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler, int? partition = null, bool blockIfQueueFull = true)
        {
            var handler = new TypedDeliveryHandlerShim(key, val, deliveryHandler);
            producer.ProduceAsync(topic, KeySerializer?.Serialize(key), ValueSerializer?.Serialize(val), handler, partition, blockIfQueueFull);
        }


        public string Name => producer.Name;

        public event EventHandler<LogArgs> OnLog;

        public event EventHandler<string> OnStatistics;

        public event EventHandler<ErrorArgs> OnError;
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
            serializingProducer = producer.Wrap(keySerializer, valueSerializer);
            producer.OnLog += (sender, e) => OnLog?.Invoke(sender, e);
            producer.OnError += (sender, e) => OnError?.Invoke(sender, e);
            producer.OnStatistics += (sender, e) => OnStatistics?.Invoke(sender, e);
        }

        public ISerializer<TKey> KeySerializer => serializingProducer.KeySerializer;

        public ISerializer<TValue> ValueSerializer => serializingProducer.ValueSerializer;

        public string Name => serializingProducer.Name;

        public Task<MessageInfo<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, int? partition = null, bool blockIfQueueFull = true)
        {
            return serializingProducer.ProduceAsync(topic, key, val, partition, blockIfQueueFull);
        }

        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler, int? partition = null, bool blockIfQueueFull = true)
        {
            serializingProducer.ProduceAsync(topic, key, val, deliveryHandler, partition, blockIfQueueFull);
        }


        public event EventHandler<LogArgs> OnLog;

        public event EventHandler<string> OnStatistics;

        public event EventHandler<ErrorArgs> OnError;

        public void Flush(int timeoutMilliseconds = int.MaxValue) => producer.Flush(TimeSpan.FromMilliseconds(timeoutMilliseconds));
        public void Flush(TimeSpan timeout) => producer.Flush(timeout);

        public void Dispose() => producer.Dispose();


        public List<GroupInfo> ListGroups(TimeSpan? timeout = null)
            => producer.ListGroups(timeout);

        public GroupInfo ListGroup(string group, TimeSpan? timeout = null)
            => producer.ListGroup(group, timeout);

        public Offsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan? timeout = null)
            => producer.QueryWatermarkOffsets(topicPartition, timeout);

        public Offsets GetWatermarkOffsets(TopicPartition topicPartition)
            => producer.GetWatermarkOffsets(topicPartition);

        public Metadata GetMetadata(string topic = null, bool includeInternal = false, TimeSpan? timeout = null)
            => producer.GetMetadata(topic, includeInternal, timeout);
    }
}
