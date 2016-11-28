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

            // TODO: some use cases may never need DeliveryReport callbacks. In this case, we may
            // be able to avoid setting this callback entirely.
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

        public Task<DeliveryReport> ProduceAsync(string topic, byte[] key, byte[] val, int? partition = null, bool blockIfQueueFull = true)
            => getKafkaTopic(topic).Produce(val, 0, val?.Length ?? 0, key, 0, key?.Length ?? 0, partition ?? RD_KAFKA_PARTITION_UA, blockIfQueueFull);

        public Task<DeliveryReport> ProduceAsync(string topic, ArraySegment<byte> key, ArraySegment<byte> val, int? partition = null, bool blockIfQueueFull = true)
            => getKafkaTopic(topic).Produce(val.Array, val.Offset, val.Count, key.Array, key.Offset, key.Count, partition ?? RD_KAFKA_PARTITION_UA, blockIfQueueFull);

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
                throw new ArgumentNullException("Key serializer must be specified.");
            }

            if (ValueSerializer == null)
            {
                throw new ArgumentNullException("Value serializer must be specified.");
            }
        }

        public Task<DeliveryReport> ProduceAsync(string topic, TKey key, TValue val, int? partition = null, bool blockIfQueueFull = true)
            => producer.ProduceAsync(topic, KeySerializer?.Serialize(key), ValueSerializer?.Serialize(val), partition, blockIfQueueFull);

        public string Name => producer.Name;
    }


    public class Producer<TKey, TValue> : ISerializingProducer<TKey, TValue>, IDisposable
    {
        private readonly Producer producer;
        private readonly ISerializingProducer<TKey, TValue> serializingProducer;


        public ISerializer<TKey> KeySerializer => serializingProducer.KeySerializer;

        public ISerializer<TValue> ValueSerializer => serializingProducer.ValueSerializer;

        public string Name => serializingProducer.Name;

        // TODO: In the future, we can introduce a new overload that takes a Message<K,V>
        // rather than T, V and add timestamp, headers etc into that.
        // TODO: put partition in there also and implement in this version.

        public Task<DeliveryReport> ProduceAsync(string topic, TKey key, TValue val, int? partition = null, bool blockIfQueueFull = true)
            => serializingProducer.ProduceAsync(topic, key, val, partition, blockIfQueueFull);

        public Producer(
            IEnumerable<KeyValuePair<string, object>> config,
            ISerializer<TKey> keySerializer,
            ISerializer<TValue> valueSerializer)
        {
            producer = new Producer(config);
            serializingProducer = producer.Wrap(keySerializer, valueSerializer);
        }

        public void Flush() => producer.Flush();

        public void Dispose() => producer.Dispose();

    }
}
