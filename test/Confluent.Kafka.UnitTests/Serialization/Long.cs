using Confluent.Kafka.Serialization;
using Xunit;

namespace Confluent.Kafka.UnitTests.Serialization
{
    public class LongTests
    {
        [Fact]
        public void SerializeDeserialize()
        {
            Assert.Equal(0, new LongDeserializer().Deserialize(new LongSerializer().Serialize(0)));
            Assert.Equal(100L, new LongDeserializer().Deserialize(new LongSerializer().Serialize(100L)));
            Assert.Equal(9223372036854775807, new LongDeserializer().Deserialize(new LongSerializer().Serialize(9223372036854775807)));
            Assert.Equal(-9223372036854775807, new LongDeserializer().Deserialize(new LongSerializer().Serialize(-9223372036854775807)));
        }
    }
}
