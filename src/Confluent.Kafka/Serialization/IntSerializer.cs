using System;

namespace Confluent.Kafka.Serialization
{
    public class IntSerializer : ISerializer<int>
    {
        /// <remark>
        ///     Endianness depends on architecture
        /// </remark>
        public byte[] Serialize(int val)
        {
            return BitConverter.GetBytes(val);
        }
    }
}
