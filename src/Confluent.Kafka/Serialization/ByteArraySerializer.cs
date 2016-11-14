using System.Text;

namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     A byte[] pass-through serializer.
    /// </summary>
    /// <param name="val">
    ///     The byte[] to pass through
    /// </param>
    /// <returns>
    ///     A reference to <paramref name="val" />.
    /// </returns>
    public class ByteArraySerializer : ISerializer<byte[]>
    {
        public byte[] Serialize(byte[] val)
        {
            return val;
        }
    }
}
