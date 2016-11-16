using System;

namespace Confluent.Kafka.Serialization
{
    public class IntSerializer : ISerializer<int>
    {
        public byte[] Serialize(int val)
        {
            return BitConverter.GetBytes(val);
        }
    }
}
