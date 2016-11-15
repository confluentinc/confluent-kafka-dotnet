using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Confluent.Kafka.Impl;

namespace Confluent.Kafka
{
    public struct DeliveryReport
    {
        public int Partition;
        public long Offset;
    }

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

        internal Topic(SafeKafkaHandle kafkaHandle, string topic, IEnumerable<KeyValuePair<string, string>> config)
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

        public Task<DeliveryReport> Produce(byte[] val, byte[] key = null, Int32 partition = RD_KAFKA_PARTITION_UA, bool blockIfQueueFull = true)
        {
            return Produce(val, val?.Length ?? 0, key, key?.Length ?? 0, partition, blockIfQueueFull);
        }

        public Task<DeliveryReport> Produce(byte[] val, int valLength, byte[] key = null, int keyCount = 0, Int32 partition = RD_KAFKA_PARTITION_UA, bool blockIfQueueFull = true)
        {
            // Passes the TaskCompletionSource to the delivery report callback
            // via the msg_opaque pointer
            var deliveryCompletionSource = new TaskDeliveryHandler();
            Produce(val, valLength, key, keyCount, partition, deliveryCompletionSource, blockIfQueueFull);
            return deliveryCompletionSource.Task;
        }

        /// <summary>
        ///     Produces a keyed message to a partition of the current Topic and notifies the caller of progress via a callback interface.
        /// </summary>
        /// <param name="val">
        ///     Value to send to Kafka. Can be null.
        /// </param>
        /// <param name="deliveryHandler">
        ///     IDeliveryHandler implementation used to notify the caller when the given produce request completes or an error occurs.
        /// </param>
        /// <param name="key">
        ///     (Optional) The key associated with <paramref name="val"/> (or null if no key is specified).
        /// </param>
        /// <param name="partition">
        ///     (Optional) The topic partition to which <paramref name="val"/> will be sent (or -1 if no partition is specified).
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///     Thrown if <paramref name="deliveryHandler"/> is null.
        /// </exception>
        /// <remarks>
        ///     Methods of <paramref name="deliveryHandler"/> will be executed in an RdKafka internal thread and will block other operations
        ///         - consider this when implementing IDeliveryHandler.
        ///     Use this overload for high-performance use cases as it does not use TPL and reduces the number of allocations.
        /// </remarks>
        public void Produce(byte[] val, IDeliveryHandler deliveryHandler, byte[] key = null, Int32 partition = RD_KAFKA_PARTITION_UA, bool blockIfQueueFull = true)
        {
            Produce(val, val?.Length ?? 0, deliveryHandler, key, key?.Length ?? 0, partition, blockIfQueueFull);
        }

        /// <summary>
        ///     Produces a keyed message to a partition of the current Topic and notifies the caller of progress via a callback interface.
        /// </summary>
        /// <param name="val">
        ///     Value to send to Kafka. Can be null.
        /// </param>
        /// <param name="valLength">
        ///     Number of bytes to use from val buffer
        /// </param>
        /// <param name="deliveryHandler">
        ///     IDeliveryHandler implementation used to notify the caller when the given produce request completes or an error occurs.
        /// </param>
        /// <param name="key">
        ///     (Optional) The key associated with <paramref name="val"/> (or null if no key is specified).
        /// </param>
        /// <param name="keyCount">
        ///     Number of bytes to use from key buffer
        /// </param>
        /// <param name="partition">
        ///     (Optional) The topic partition to which <paramref name="val"/> will be sent (or -1 if no partition is specified).
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///     Thrown if <paramref name="deliveryHandler"/> is null.
        /// </exception>
        /// <remarks>
        ///     Methods of <paramref name="deliveryHandler"/> will be executed in an RdKafka-internal thread and will block other operations
        ///         - consider this when implementing IDeliveryHandler.
        ///     Use this overload for high-performance use cases as it does not use TPL and reduces the number of allocations.
        /// </remarks>
        public void Produce(byte[] value, int valLength, IDeliveryHandler deliveryHandler, byte[] key = null, int keyCount = 0, Int32 partition = RD_KAFKA_PARTITION_UA, bool blockIfQueueFull = true)
        {
            if (deliveryHandler == null)
                throw new ArgumentNullException(nameof(deliveryHandler));
            Produce(value, valLength, key, keyCount, partition, deliveryHandler, blockIfQueueFull);
        }


        private void Produce(byte[] val, int valLength, byte[] key, int keyCount, Int32 partition, object deliveryHandler, bool blockIfQueueFull)
        {
            var gch = GCHandle.Alloc(deliveryHandler);
            var ptr = GCHandle.ToIntPtr(gch);

            if (handle.Produce(val, valLength, key, keyCount, partition, ptr, blockIfQueueFull) != 0)
            {
                var err = LibRdKafka.last_error();
                gch.Free();
                throw RdKafkaException.FromErr(err, "Could not produce message");
            }
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
