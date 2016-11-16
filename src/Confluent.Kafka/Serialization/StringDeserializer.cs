using System.Text;

namespace Confluent.Kafka.Serialization
{
    public class StringDeserializer : IDeserializer<string>
    {
        Encoding encoding;

        StringDeserializer()
        {
            encoding = Encoding.UTF8;
        }

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
