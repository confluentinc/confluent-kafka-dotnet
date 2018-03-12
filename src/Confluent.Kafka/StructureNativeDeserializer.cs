using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Confluent.Kafka
{
    public class StructureNativeDeserializer<T> : INativeDeserializer<T>
    {
//        private readonly Type _typeofT = typeof(T);

        private readonly int _sizeofT = Marshal.SizeOf(typeof(T));
        public unsafe T Deserialize(IntPtr msgBuf, uint msgLen, IntPtr msgTopic)
        {
            if (msgLen < _sizeofT)
                throw new ArgumentException("Data length not sufficient.");
            return Unsafe.Read<T>(msgBuf.ToPointer());
//            return (T) Marshal.PtrToStructure(msgBuf, _typeofT);
        }
    }
}