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
            var co = new CommittedOffsets(tpos, ErrorCode._AUTHENTICATION);
            Assert.Same(co.Offsets, tpos);
            Assert.Equal(co.Error, ErrorCode._AUTHENTICATION);
        }
    }
}
