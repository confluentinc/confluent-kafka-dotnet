using System;


namespace Confluent.Kafka.Serialization
{
    public class IntDeserializer : IDeserializer<int>
    {
        public int Deserialize(byte[] data)
        {
            return BitConverter.ToInt32(data, 0);
        }
    }
}
