using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.Tests
{
    public class MetadataTests
    {
        [Fact]
        public void Constuctor()
        {
            var brokers = new List<BrokerMetadata>();
            var topics = new List<TopicMetadata>();
            var md = new Metadata(brokers, topics, 42, "broker1");
            Assert.Same(md.Brokers, brokers);
            Assert.Same(md.Topics, topics);
            Assert.Equal(md.OriginatingBrokerId, 42);
            Assert.Equal(md.OriginatingBrokerName, "broker1");
        }

        // TODO: ToString() tests. Note: there is coverage of this already in the Metdata integration test.
    }
}
