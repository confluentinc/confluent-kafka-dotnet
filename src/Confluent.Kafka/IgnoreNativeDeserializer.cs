using System;

namespace Confluent.Kafka
{
    public class IgnoreNativeDeserializer<T> : INativeDeserializer<T>
    {
        public T Deserialize(IntPtr msgBuf, uint msgLen, IntPtr msgTopic)
        {
            return default(T);
        }
    }
}