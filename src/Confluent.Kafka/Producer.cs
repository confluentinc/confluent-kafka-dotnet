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
    /// <summary>
    ///     High-level, asynchronous message producer.
    /// </summary>
    public class Producer<TKey, TValue> : Handle
    {
        // TODO: allow be be set only in constructor. make readonly.
        // TODO: These should arguably be left as properties here.
        // TODO: And/or these could be in <string,string> config + use reflection to find the class.
        public ISerializer<TKey> KeySerializer { get; set; }

        // TODO: allow be be set only in constructor. make readonly.
        public ISerializer<TValue> ValueSerializer { get; set; }

        private IEnumerable<KeyValuePair<string, string>> topicConfig;
        // TODO: get rid of the Topic class.
        private SafeDictionary<string, Topic> topics = new SafeDictionary<string, Topic>();

        // TODO: merge topicConfig with config.
        public Producer(IEnumerable<KeyValuePair<string, string>> config, IEnumerable<KeyValuePair<string, string>> topicConfig)
        {
            this.topicConfig = topicConfig;
            var rdKafkaConfig = new Config(config.Where(a => a.Key != "bootstrap.servers"));
            var bootstrapServers = config.FirstOrDefault(a => a.Key == "bootstrap.servers").Value;

            // TODO: If serializers aren't specified in config, then we could use defaults associated with TKey, TValue if
            //       we have matching Confluent.Kafka.Serialization serializers available.

            IntPtr cfgPtr = rdKafkaConfig.handle.Dup();
            LibRdKafka.conf_set_dr_msg_cb(cfgPtr, DeliveryReportDelegate);
            Init(RdKafkaType.Producer, cfgPtr, rdKafkaConfig.Logger);

            if (bootstrapServers != null)
            {
                handle.AddBrokers(bootstrapServers);
            }
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


        // TODO: Support the other function overloads in Topic.
        // TODO: I'd like a way to produce as (byte[], offset, length) as well if possible all the way down to librdkafka (need to investigate).
        // TODO: should we append the Produce methods with Async? Seems to be a convention.

        public Task<DeliveryReport> Produce(string topic, TValue val)
            => getKafkaTopic(topic).Produce(ValueSerializer.Serialize(val));

        public Task<DeliveryReport> Produce(string topic, TKey key, TValue val)
            => getKafkaTopic(topic).Produce(ValueSerializer.Serialize(val), KeySerializer.Serialize(key));

        // TODO: do we need both the callback way of doing this and the Task way?
        //       i think this was added late to rdkafka-dotnet, so there is probably a need.
        //       What about Task.ContinueWith? I belive this can even handle exceptions?
        public void ProduceWithDeliveryReport(string topic, TValue val, IDeliveryHandler deliveryHandler)
            => getKafkaTopic(topic).Produce(ValueSerializer.Serialize(val), deliveryHandler);

        // Explicitly keep reference to delegate so it stays alive
        private static readonly LibRdKafka.DeliveryReportCallback DeliveryReportDelegate = DeliveryReportCallback;

        private static void DeliveryReportCallback(IntPtr rk, ref rd_kafka_message rkmessage, IntPtr opaque)
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

            deliveryHandler.SetResult(new DeliveryReport {
                Offset = rkmessage.offset,
                Partition = rkmessage.partition
            });
        }
    }
}
