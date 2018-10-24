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
            Assert.Equal(values, Deserializers.ByteArray(Serializers.ByteArray(values), false));
        }

        [Fact]
        public void CanReconstructByteArrayNull()
        {
            Assert.Null(Deserializers.ByteArray(Serializers.ByteArray(null), true));
        }
    }
}
