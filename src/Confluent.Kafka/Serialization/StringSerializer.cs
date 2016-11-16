using System.Text;

namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     String serializer.
    /// </summary>
    /// <param name="val">
    ///     The string value to serialize.
    /// </param>
    /// <returns>
    ///     <paramref name="val" /> encoded in a byte array.
    /// </returns>
    /// <remarks>
    ///     <paramref name="val" /> cannot be null.
    ///     TODO: well it shouldn't be other there is ambiguity on deserialization. check this.
    /// </remarks>
    public class StringSerializer : ISerializer<string>
    {
        private Encoding encoding;

        public StringSerializer()
        {
            encoding = Encoding.UTF8;
        }

        public StringSerializer(Encoding encoding)
        {
            this.encoding = encoding;
        }

        public byte[] Serialize(string val)
        {
            return encoding.GetBytes(val);
        }
    }
}
