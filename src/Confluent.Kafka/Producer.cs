using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka
{
    /// <summary>
    ///     High-level, asynchronous message producer.
    /// </summary>
    public class Producer : Handle
    {
        public Producer(IEnumerable<KeyValuePair<string, string>> config)
        {
            var rdKafkaConfig = new Config(config.Where(a => a.Key != "bootstrap.servers"));
            var bootstrapServers = config.FirstOrDefault(a => a.Key == "bootstrap.servers").Value;

            IntPtr cfgPtr = rdKafkaConfig.handle.Dup();
            LibRdKafka.conf_set_dr_msg_cb(cfgPtr, DeliveryReportDelegate);
            Init(RdKafkaType.Producer, cfgPtr, rdKafkaConfig.Logger);

            if (bootstrapServers != null)
            {
                handle.AddBrokers(bootstrapServers);
            }
        }


        public Topic Topic(string topic, IEnumerable<KeyValuePair<string, string>> config = null) => new Topic(handle, this, topic, config);

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
