using Confluent.Kafka.Serialization;
using Xunit;


namespace Confluent.Kafka.UnitTests.Serialization
{
    public class ByteArrayTests
    {
        [Theory]
        [InlineData(new byte[0])]
        [InlineData(new byte[] { 1 })]
        [InlineData(new byte[] { 1, 2, 3, 4, 5 })]
        public void CanReconstructByteArray(byte[] values)
        {
            Assert.Equal(values, new ByteArrayDeserializer().Deserialize(null, new ByteArraySerializer().Serialize(null, values), false));
        }

        public void CanReconstructByteArrayNull()
        {
            Assert.Equal(null, new ByteArrayDeserializer().Deserialize(null, new ByteArraySerializer().Serialize(null, null), true));
        }
    }
}
