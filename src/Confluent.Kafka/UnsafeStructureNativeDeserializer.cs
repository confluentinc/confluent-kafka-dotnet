using System;
using System.Runtime.CompilerServices;

namespace Confluent.Kafka
{
    public class UnsafeStructureNativeDeserializer<T> : INativeDeserializer<T>
    {
        public unsafe T Deserialize(IntPtr msgBuf, uint msgLen, IntPtr msgTopic)
        {
            return Unsafe.Read<T>(msgBuf.ToPointer());
        }
    }
}