using Xunit;
using System.Collections.Generic;


namespace Confluent.Kafka.Tests
{
    public class TopicPartitionOffsetTests
    {
        [Fact]
        public void Constuctor()
        {
            var tpo = new TopicPartitionOffset("mytopic", 42, 107);
            Assert.Equal(tpo.Topic, "mytopic");
            Assert.Equal(tpo.Partition, 42);
            Assert.Equal(tpo.Offset, 107);
        }

        [Fact]
        public void Equality()
        {
            var a = new TopicPartitionOffset("a", 31, 55);
            var a2 = new TopicPartitionOffset("a", 31, 55);
            var nes = new List<TopicPartitionOffset> {
                new TopicPartitionOffset("b", 31, 55),
                new TopicPartitionOffset("a", 32, 55),
                new TopicPartitionOffset("a", 31, 56),
            };

            Assert.Equal(a, a2);
            Assert.True(a.Equals(a2));
            Assert.True(a == a2);
            Assert.False(a != a2);

            foreach (var ne in nes)
            {
                Assert.NotEqual(a, ne);
                Assert.False(a.Equals(ne));
                Assert.False(a == ne);
                Assert.True(a != ne);
            }
        }

        [Fact]
        public void ToStringTest()
        {
            var tpo = new TopicPartitionOffset("mytopic", 42, 107);
            Assert.True(tpo.ToString().Contains(tpo.Topic));
            Assert.True(tpo.ToString().Contains(tpo.Partition.ToString()));
            Assert.True(tpo.ToString().Contains(tpo.Offset.ToString()));
        }

        [Fact]
        public void Properties()
        {
            var tpo = new TopicPartitionOffset("mytopic", 42, 107);
            Assert.Equal(tpo.TopicPartition, new TopicPartition("mytopic", 42));
        }
    }
}
