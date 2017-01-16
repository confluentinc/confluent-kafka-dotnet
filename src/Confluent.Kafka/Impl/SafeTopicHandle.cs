using System;
using System.Runtime.InteropServices;
using Confluent.Kafka.Internal;


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

        private SafeTopicHandle() {}

        protected override bool ReleaseHandle()
        {
            LibRdKafka.topic_destroy(handle);
            // See SafeKafkaHandle.Topic
            kafkaHandle.DangerousRelease();
            return true;
        }

        internal string GetName()
            => Util.Marshal.PtrToStringUTF8(LibRdKafka.topic_name(handle));

        internal long Produce(byte[] val, int valOffset, int valLength, byte[] key, int keyOffset, int keyLength, int partition, long? timestamp, IntPtr opaque, bool blockIfQueueFull)
        {
            var pValue = IntPtr.Zero;
            var pKey = IntPtr.Zero;

            var gchValue = default(GCHandle);
            var gchKey = default(GCHandle);

            if (val != null)
            {
                gchValue = GCHandle.Alloc(val, GCHandleType.Pinned);
                pValue = Marshal.UnsafeAddrOfPinnedArrayElement(val, valOffset);
            }

            if (key != null)
            {
                gchKey = GCHandle.Alloc(key, GCHandleType.Pinned);
                pKey = Marshal.UnsafeAddrOfPinnedArrayElement(key, keyOffset);
            }

            try
            {
                // TODO: when refactor complete, reassess the below note.
                // Note: since the message queue threshold limit also includes delivery reports, it is important that another
                // thread of the application calls poll() for a blocking produce() to ever unblock.
                if (timestamp == null)
                {
                    return (long) LibRdKafka.produce(
                        handle,
                        partition,
                        (IntPtr) (MsgFlags.MSG_F_COPY | (blockIfQueueFull ? MsgFlags.MSG_F_BLOCK : 0)),
                        pValue, (UIntPtr) valLength,
                        pKey, (UIntPtr) keyLength,
                        opaque);
                }
                else
                {
                    return (long) LibRdKafka.producev(
                        handle,
                        partition,
                        (IntPtr) (MsgFlags.MSG_F_COPY | (blockIfQueueFull ? MsgFlags.MSG_F_BLOCK : 0)),
                        pValue, (UIntPtr) valLength,
                        pKey, (UIntPtr) keyLength,
                        timestamp.Value,
                        opaque);
                }
            }
            finally
            {
                if (val != null)
                {
                    gchValue.Free();
                }

                if (key != null)
                {
                    gchKey.Free();
                }
            }
        }

        internal bool PartitionAvailable(int partition)
            => LibRdKafka.topic_partition_available(handle, partition);
    }
}
