using System;
using System.Runtime.InteropServices;

namespace Confluent.Kafka
{
    public class StructureNativeDeserializer<T> : INativeDeserializer<T>
    {
        private readonly Type _typeofT = typeof(T);

        public T Deserialize(IntPtr msgBuf, uint msgLen, IntPtr msgTopic)
        {
            return (T) Marshal.PtrToStructure(msgBuf, _typeofT);
        }
    }
}