using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.Tests
{
    public class TopicMetadataTests
    {
        [Fact]
        public void Constuctor()
        {
            var partitions = new List<PartitionMetadata>();
            var tm = new TopicMetadata("mytopic", partitions, ErrorCode._ALL_BROKERS_DOWN);

            Assert.Equal(tm.Topic, "mytopic");
            Assert.Same(partitions, tm.Partitions);
            Assert.Equal(tm.Error, new Error(ErrorCode._ALL_BROKERS_DOWN));
        }

        // TODO: ToString() tests. Note: there is coverage of this already in the Metdata integration test.
    }
}
