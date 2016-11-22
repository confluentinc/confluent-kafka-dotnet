using System;


namespace Confluent.Kafka.Serialization
{
    public class IntDeserializer : IDeserializer<int>
    {
        /// <remark>
        ///     Endianness depends on architecture
        /// </remark>
        public int Deserialize(byte[] data)
        {
            return BitConverter.ToInt32(data, 0);
        }
    }
}
