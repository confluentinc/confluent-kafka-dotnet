using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.Tests
{
    public class CommittedOffetsTest
    {
        [Fact]
        public void Constuctor()
        {
            var tpos = new List<TopicPartitionOffset>();
            var err = new Error(ErrorCode.UNKNOWN_TOPIC_OR_PART);
            var co = new CommittedOffsets(tpos, err);
            Assert.Same(co.Offsets, tpos);
            Assert.Equal(co.Error, err);
        }
    }
}
