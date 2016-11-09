using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using RdKafka.Internal;

namespace RdKafka
{
    public struct DeliveryReport
    {
        public int Partition;
        public long Offset;
    }

    /// <summary>
    /// Handle to a topic obtained from <see cref="Producer" />.
    /// </summary>
    public class Topic : IDisposable
    {
        private sealed class TaskDeliveryHandler : TaskCompletionSource<DeliveryReport>, IDeliveryHandler
        {
        }
        
        const int RD_KAFKA_PARTITION_UA = -1;

        internal readonly SafeTopicHandle handle;
        readonly Producer producer;
        readonly LibRdKafka.PartitionerCallback PartitionerDelegate;

        internal Topic(SafeKafkaHandle kafkaHandle, Producer producer, string topic, TopicConfig config)
        {
            this.producer = producer;

            config = config ?? new TopicConfig();
            config["produce.offset.report"] = "true";
            IntPtr configPtr = config.handle.Dup();

            if (config.CustomPartitioner != null)
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
                    return config.CustomPartitioner(this, key, partition_cnt);
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

        public Task<DeliveryReport> Produce(byte[] payload, byte[] key = null, Int32 partition = RD_KAFKA_PARTITION_UA)
        {
            return Produce(payload, payload?.Length ?? 0, key, key?.Length ?? 0, partition);
        }

        public Task<DeliveryReport> Produce(byte[] payload, int payloadCount, byte[] key = null, int keyCount = 0, Int32 partition = RD_KAFKA_PARTITION_UA)
        {
            // Passes the TaskCompletionSource to the delivery report callback
            // via the msg_opaque pointer
            var deliveryCompletionSource = new TaskDeliveryHandler();
            Produce(payload, payloadCount, key, keyCount, partition, deliveryCompletionSource);
            return deliveryCompletionSource.Task;
        }

        /// <summary>
        /// Produces a keyed message to a partition of the current Topic and notifies the caller of progress via a callback interface.
        /// </summary>
        /// <param name="payload">Payload to send to Kafka. Can be null.</param>
        /// <param name="deliveryHandler">IDeliveryHandler implementation used to notify the caller when the given produce request completes or an error occurs.</param>
        /// <param name="key">(Optional) The key associated with <paramref name="payload"/> (or null if no key is specified).</param>
        /// <param name="partition">(Optional) The topic partition to which <paramref name="payload"/> will be sent (or -1 if no partition is specified).</param>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="deliveryHandler"/> is null.</exception>
        /// <remarks>Methods of <paramref name="deliveryHandler"/> will be executed in an RdKafka-internal thread and will block other operations - consider this when implementing IDeliveryHandler.
        /// Use this overload for high-performance use cases as it does not use TPL and reduces the number of allocations.</remarks>
        public void Produce(byte[] payload, IDeliveryHandler deliveryHandler, byte[] key = null, Int32 partition = RD_KAFKA_PARTITION_UA)
        {
            Produce(payload, payload?.Length ?? 0, deliveryHandler, key, key?.Length ?? 0, partition);
        }

        /// <summary>
        /// Produces a keyed message to a partition of the current Topic and notifies the caller of progress via a callback interface.
        /// </summary>
        /// <param name="payload">Payload to send to Kafka. Can be null.</param>
        /// <param name="payloadCount">Number of bytes to use from payload buffer</param>
        /// <param name="deliveryHandler">IDeliveryHandler implementation used to notify the caller when the given produce request completes or an error occurs.</param>
        /// <param name="key">(Optional) The key associated with <paramref name="payload"/> (or null if no key is specified).</param>
        /// <param name="keyCount">Number of bytes to use from key buffer</param>
        /// <param name="partition">(Optional) The topic partition to which <paramref name="payload"/> will be sent (or -1 if no partition is specified).</param>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="deliveryHandler"/> is null.</exception>
        /// <remarks>Methods of <paramref name="deliveryHandler"/> will be executed in an RdKafka-internal thread and will block other operations - consider this when implementing IDeliveryHandler.
        /// Use this overload for high-performance use cases as it does not use TPL and reduces the number of allocations.</remarks>
        public void Produce(byte[] payload, int payloadCount, IDeliveryHandler deliveryHandler, byte[] key = null, int keyCount = 0, Int32 partition = RD_KAFKA_PARTITION_UA)
        {
            if (deliveryHandler == null)
                throw new ArgumentNullException(nameof(deliveryHandler));
            Produce(payload, payloadCount, key, keyCount, partition, deliveryHandler);
        }


        private void Produce(byte[] payload, int payloadCount, byte[] key, int keyCount, Int32 partition, object deliveryHandler)
        {
            var gch = GCHandle.Alloc(deliveryHandler);
            var ptr = GCHandle.ToIntPtr(gch);

            while (true)
            {
                if (handle.Produce(payload, payloadCount, key, keyCount, partition, ptr) == 0)
                {
                    // Successfully enqueued produce request
                    break;
                }

                var err = LibRdKafka.last_error();
                if (err == ErrorCode._QUEUE_FULL)
                {
                    // Wait and retry
                    Task.Delay(TimeSpan.FromMilliseconds(50)).Wait();
                }
                else
                {
                    gch.Free();
                    throw RdKafkaException.FromErr(err, "Could not produce message");
                }
            }
        }

        /// <summary>
        /// Check if partition is available (has a leader broker).
        ///
        /// Return true if the partition is available, else false.
        ///
        /// This function must only be called from inside a partitioner function.
        /// </summary>
        public bool PartitionAvailable(int partition) => handle.PartitionAvailable(partition);
    }
}
