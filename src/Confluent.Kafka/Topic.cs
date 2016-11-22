using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Confluent.Kafka.Impl;

namespace Confluent.Kafka
{
    // TODO: We want to get rid of this class entirely.

    /// <summary>
    ///     Handle to a topic obtained from <see cref="Producer" />.
    /// </summary>
    public class Topic : IDisposable
    {
        private sealed class TaskDeliveryHandler : TaskCompletionSource<DeliveryReport>, IDeliveryHandler
        {
        }

        const int RD_KAFKA_PARTITION_UA = -1;

        internal readonly SafeTopicHandle handle;
        readonly LibRdKafka.PartitionerCallback PartitionerDelegate;

        internal Topic(SafeKafkaHandle kafkaHandle, string topic, IEnumerable<KeyValuePair<string, object>> config)
        {
            var rdKafkaTopicConfig = new TopicConfig(config);
            rdKafkaTopicConfig["produce.offset.report"] = "true";

            IntPtr configPtr = rdKafkaTopicConfig.handle.Dup();

            // FIXME (mhowlett): CustomPartitioner currently can't be set.
            if (rdKafkaTopicConfig.CustomPartitioner != null)
            {
                PartitionerDelegate = (IntPtr rkt, IntPtr keydata, UIntPtr keylen, int partition_cnt,
                        IntPtr rkt_opaque, IntPtr msg_opaque) =>
                {
                    byte[] key = null;
                    if (keydata != IntPtr.Zero)
                    {
                        key = new byte[(int) keylen];
                        Marshal.Copy(keydata, key, 0, (int) keylen);
                    }
                    return rdKafkaTopicConfig.CustomPartitioner(this, key, partition_cnt);
                };
                LibRdKafka.topic_conf_set_partitioner_cb(configPtr, PartitionerDelegate);
            }

            handle = kafkaHandle.Topic(topic, configPtr);
        }

        public void Dispose()
        {
            handle.Dispose();
        }

        public string Name => handle.GetName();

        public Task<DeliveryReport> Produce(byte[] val, int valOffset, int valLength, byte[] key = null, int keyOffset = 0, int keyLength = 0, Int32 partition = RD_KAFKA_PARTITION_UA, bool blockIfQueueFull = true)
        {
            // Passes the TaskCompletionSource to the delivery report callback via the msg_opaque pointer

            var deliveryCompletionSource = new TaskDeliveryHandler();
            var gch = GCHandle.Alloc(deliveryCompletionSource);
            var ptr = GCHandle.ToIntPtr(gch);

            if (handle.Produce(val, valOffset, valLength, key, keyOffset, keyLength, partition, ptr, blockIfQueueFull) != 0)
            {
                var err = LibRdKafka.last_error();
                gch.Free();
                // TODO: Use proper error string (rd_kafka_err2str(..last_error))
                throw RdKafkaException.FromErr(err, "Could not produce message");
            }

            return deliveryCompletionSource.Task;
        }

        /// <summary>
        ///     Check if partition is available (has a leader broker).
        ///
        ///     Return true if the partition is available, else false.
        ///
        ///     This function must only be called from inside a partitioner function.
        /// </summary>
        public bool PartitionAvailable(int partition) => handle.PartitionAvailable(partition);

    }
}
