using System.Text;

// TODO: Deserializer needs to be able to handle nulls and differentiate between null and empty string.


namespace Confluent.Kafka.Serialization
{
    public class StringDeserializer : IDeserializer<string>
    {
        Encoding encoding;

        StringDeserializer(Encoding encoding)
        {
            this.encoding = encoding;
        }

        public string Deserialize(byte[] data)
        {
            return Encoding.UTF8.GetString(data);
        }
    }
}
