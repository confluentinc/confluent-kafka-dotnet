using Xunit;


namespace Confluent.Kafka.Tests
{
    public class WatermarkOffsetTests
    {
        [Fact]
        public void Constuctor()
        {
            var wo = new WatermarkOffsets(42, 43);
            Assert.Equal(wo.Low, new Offset(42));
            Assert.Equal(wo.High, new Offset(43));
        }
    }
}
