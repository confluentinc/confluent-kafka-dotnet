using System;

namespace Confluent.Kafka
{
    public interface INativeDeserializer<T>
    {
        T Deserialize(IntPtr msgBuf, uint msgLen, IntPtr msgTopic);
    }
}