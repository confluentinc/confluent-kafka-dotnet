using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using System.Runtime.InteropServices;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Serialization;

namespace Confluent.Kafka
{
    public class Producer : Handle
    {
        private const int RD_KAFKA_PARTITION_UA = -1;

        private IEnumerable<KeyValuePair<string, object>> topicConfig;

        // TODO: get rid of the Topic class.
        private SafeDictionary<string, Topic> topics = new SafeDictionary<string, Topic>();

        public Producer(IEnumerable<KeyValuePair<string, object>> config)
        {
            this.topicConfig = (IEnumerable<KeyValuePair<string, object>>)config.FirstOrDefault(prop => prop.Key == "default.topic.config").Value;

            // setup the rdkafka config object, including pointer to the delivery report callback.
            var rdKafkaConfig = new Config(config.Where(prop => prop.Key != "default.topic.config"));
            IntPtr rdKafkaConfigPtr = rdKafkaConfig.handle.Dup();
            // TODO: we might want to defer setting the report callback until we know we actually need it.
            LibRdKafka.conf_set_dr_msg_cb(rdKafkaConfigPtr, DeliveryReportCallback);

            Init(RdKafkaType.Producer, rdKafkaConfigPtr, rdKafkaConfig.Logger);
        }

        private Topic getKafkaTopic(string topic)
        {
            if (topics.ContainsKey(topic))
            {
                return topics[topic];
            }

            // TODO: get rid of the Topic class - just use handles.
            var kafkaTopic = new Topic(handle, topic, topicConfig);
            topics.Add(topic, kafkaTopic);
            return kafkaTopic;
        }

        private static readonly LibRdKafka.DeliveryReportCallback DeliveryReportCallback = DeliveryReportCallbackImpl;

        private static void DeliveryReportCallbackImpl(IntPtr rk, ref rd_kafka_message rkmessage, IntPtr opaque)
        {
            if (rkmessage._private == IntPtr.Zero)
            {
                return;
            }

            // msg_opaque was set by Topic.Produce
            var gch = GCHandle.FromIntPtr(rkmessage._private);
            var deliveryHandler = (IDeliveryHandler) gch.Target;
            gch.Free();

            if (rkmessage.err != 0)
            {
                deliveryHandler.SetException(
                    RdKafkaException.FromErr(
                        rkmessage.err,
                        "Failed to produce message"));
                return;
            }

            deliveryHandler.SetResult(
                new DeliveryReport
                {
                    Offset = rkmessage.offset,
                    Partition = rkmessage.partition
                }
            );
        }

        public ISerializingProducer<TKey, TValue> Wrap<TKey, TValue>(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
        {
            return new SerializingProducer<TKey, TValue>(this, keySerializer, valueSerializer);
        }

        public Task<DeliveryReport> ProduceAsync(string topic, byte[] val, int? partition = null, bool blockIfQueueFull = true)
            => getKafkaTopic(topic).Produce(val, 0, val.Length, null, 0, 0, partition ?? RD_KAFKA_PARTITION_UA, blockIfQueueFull);

        public Task<DeliveryReport> ProduceAsync(string topic, byte[] key, byte[] val, int? partition = null, bool blockIfQueueFull = true)
            => getKafkaTopic(topic).Produce(val, 0, val.Length, key, 0, key.Length, partition ?? RD_KAFKA_PARTITION_UA, blockIfQueueFull);

        public Task<DeliveryReport> ProduceAsync(string topic, ArraySegment<byte> key, ArraySegment<byte> val, int? partition = null, bool blockIfQueueFull = true)
            => getKafkaTopic(topic).Produce(val.Array, val.Offset, val.Count, key.Array, key.Offset, key.Count, partition ?? RD_KAFKA_PARTITION_UA, blockIfQueueFull);

        // There are (or have been) 3 different patterns for doing async requests in .NET (two of them depreciated).
        // https://msdn.microsoft.com/en-us/library/jj152938(v=vs.110).aspx
        // None of them are to use a callback (as below), but I believe this makes the most sense in the current situation.
        // A reason we might want these methods is the callbacks all happen on the same thread, whereas the Task completion ones don't.
        // This is used by the benchmark example to avoid locking the counter (maybe this is a common thing we want to do?)
        public void ProduceAsync(string topic, byte[] key, byte[] val, IDeliveryHandler deliveryHandler = null, int? partition = null, bool blockIfQueueFull = true)
            => getKafkaTopic(topic).Produce(val, 0, val.Length, deliveryHandler, key, 0, key.Length, partition ?? RD_KAFKA_PARTITION_UA, blockIfQueueFull);

        public void ProduceAsync(string topic, ArraySegment<byte> key, ArraySegment<byte> val, IDeliveryHandler deliveryHandler = null, int? partition = null, bool blockIfQueueFull = true)
            => getKafkaTopic(topic).Produce(val.Array, val.Offset, val.Count, deliveryHandler, key.Array, key.Offset, key.Count, partition ?? RD_KAFKA_PARTITION_UA, blockIfQueueFull);
    }


    internal class SerializingProducer<TKey, TValue> : ISerializingProducer<TKey, TValue>
    {
        protected Producer producer;

        public ISerializer<TKey> KeySerializer { get; }

        public ISerializer<TValue> ValueSerializer { get; }

        public SerializingProducer(Producer producer, ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
        {
            this.producer = producer;
            KeySerializer = keySerializer;
            ValueSerializer = valueSerializer;

            // TODO: specify serializers via config file as well? In which case, default keySerializer and valueSerizlizer params to null.

            if (KeySerializer == null)
            {
                throw new ArgumentNullException("Key serializer must be specified.");
            }

            if (ValueSerializer == null)
            {
                throw new ArgumentNullException("Value serializer must be specified.");
            }
        }

        public Task<DeliveryReport> ProduceAsync(string topic, TValue val, int? partition = null, bool blockIfQueueFull = true)
            => producer.ProduceAsync(topic, ValueSerializer.Serialize(val), partition, blockIfQueueFull);

        public Task<DeliveryReport> ProduceAsync(string topic, TKey key, TValue val, int? partition = null, bool blockIfQueueFull = true)
            => producer.ProduceAsync(topic, KeySerializer.Serialize(key), ValueSerializer.Serialize(val), partition, blockIfQueueFull);

        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler deliveryHandler = null, int? partition = null, bool blockIfQueueFull = true)
            => producer.ProduceAsync(topic, KeySerializer.Serialize(key), ValueSerializer.Serialize(val), deliveryHandler, partition, blockIfQueueFull);

        public string Name => producer.Name;
    }


    public class Producer<TKey, TValue> : ISerializingProducer<TKey, TValue>, IDisposable
    {
        private Producer producer;
        private ISerializingProducer<TKey, TValue> serializingProducer;


        public ISerializer<TKey> KeySerializer => serializingProducer.KeySerializer;

        public ISerializer<TValue> ValueSerializer => serializingProducer.ValueSerializer;

        public string Name => serializingProducer.Name;

        public Task<DeliveryReport> ProduceAsync(string topic, TValue val, int? partition = null, bool blockIfQueueFull = true)
            => serializingProducer.ProduceAsync(topic, val, partition, blockIfQueueFull);

        public Task<DeliveryReport> ProduceAsync(string topic, TKey key, TValue val, int? partition = null, bool blockIfQueueFull = true)
            => serializingProducer.ProduceAsync(topic, key, val, partition, blockIfQueueFull);

        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler deliveryHandler = null, int? partition = null, bool blockIfQueueFull = true)
            => serializingProducer.ProduceAsync(topic, key, val, deliveryHandler, partition, blockIfQueueFull);

        public Producer(
            IEnumerable<KeyValuePair<string, object>> config,
            ISerializer<TKey> keySerializer,
            ISerializer<TValue> valueSerializer)
        {
            producer = new Producer(config);
            serializingProducer = producer.Wrap(keySerializer, valueSerializer);
        }

        public void Dispose()
        {
            producer.Dispose();
        }
    }
}
