using System.Text;

namespace Confluent.Kafka.Serialization
{
    public class Utf8StringDeserializer : IDeserializer<string>
    {
        public string Deserialize(byte[] data)
        {
            return Encoding.UTF8.GetString(data);
        }
    }
}
