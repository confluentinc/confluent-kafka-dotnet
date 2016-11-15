using System;
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Impl
{
    enum MsgFlags
    {
        MSG_F_FREE = 1,
        MSG_F_COPY = 2,
        MSG_F_BLOCK = 4
    }

    internal sealed class SafeTopicHandle : SafeHandleZeroIsInvalid
    {
        const int RD_KAFKA_PARTITION_UA = -1;

        internal SafeKafkaHandle kafkaHandle;

        private SafeTopicHandle() { }

        protected override bool ReleaseHandle()
        {
            LibRdKafka.topic_destroy(handle);
            // See SafeKafkaHandle.Topic
            kafkaHandle.DangerousRelease();
            return true;
        }

        internal string GetName() => Marshal.PtrToStringAnsi(LibRdKafka.topic_name(handle));

        internal long Produce(byte[] val, int valLength, byte[] key, int keyCount, int partition, IntPtr opaque, bool blockIfQueueFull)
            => (long) LibRdKafka.produce(
                    handle,
                    partition,
                    (IntPtr) (MsgFlags.MSG_F_COPY | (blockIfQueueFull ? MsgFlags.MSG_F_BLOCK : 0)),
                    val, (UIntPtr) valLength,
                    key, (UIntPtr) keyCount,
                    opaque);

        internal bool PartitionAvailable(int partition) => LibRdKafka.topic_partition_available(handle, partition);
    }
}
