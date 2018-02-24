using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Confluent.Kafka
{
    public class LongNativeDeserializer : INativeDeserializer<long>
    {
        public unsafe long Deserialize(IntPtr msgBuf, uint msgLen, IntPtr msgTopic)
        {
            if (msgLen < 8)
                throw new ArgumentException("Data length not sufficient.");
            return Unsafe.Read<Int64>(msgBuf.ToPointer());
        }
    }
}