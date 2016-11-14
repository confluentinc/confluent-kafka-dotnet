using System.Text;

namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     A UTF-8 encoding string serializer.
    /// </summary>
    /// <param name="val">
    ///     The string value to serialize.
    /// </param>
    /// <returns>
    ///     <paramref name="val" /> encoded in a UTF-8 byte array.
    /// </returns>
    /// <remarks>
    ///     <paramref name="val" /> cannot be null.
    /// </remarks>
    public class Utf8StringSerializer : ISerializer<string>
    {
        public byte[] Serialize(string val)
        {
            return Encoding.UTF8.GetBytes(val);
        }
    }
}
