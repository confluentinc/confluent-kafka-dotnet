using Xunit;


namespace Confluent.Kafka.Tests
{
    public class BrokerMetadataTest
    {
        [Fact]
        public void Constuctor()
        {
            var bm = new BrokerMetadata(42, "myhost", 8080);
            Assert.Equal(bm.BrokerId, 42);
            Assert.Equal(bm.Host, "myhost");
            Assert.Equal(bm.Port, 8080);
        }

        [Fact]
        public void ToStringTest()
        {
            var bm = new BrokerMetadata(42, "myhost", 8080);
            Assert.True(bm.ToString().Contains(42.ToString()));
            Assert.True(bm.ToString().Contains("myhost"));
            Assert.True(bm.ToString().Contains(8080.ToString()));

            // TODO: JSON based test. Note: there is coverage of this already in the Metdata integration test.
        }
    }
}

