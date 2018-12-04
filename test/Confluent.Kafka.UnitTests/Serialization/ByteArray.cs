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
            Assert.Equal(values, Deserializers.ByteArray.Deserialize(Serializers.ByteArray.Serialize(values, false, null, null), false, false, null, null));
        }

        [Fact]
        public void CanReconstructByteArrayNull()
        {
            Assert.Null(Deserializers.ByteArray.Deserialize(Serializers.ByteArray.Serialize(null, false, null, null), true, false, null, null));
        }
    }
}
