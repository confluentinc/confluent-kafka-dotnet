using Xunit;
using System.Collections.Generic;


namespace Confluent.Kafka.Tests
{
    public class TopicPartitionTests
    {
        [Fact]
        public void Constuctor()
        {
            var tp = new TopicPartition("mytopic", 42);
            Assert.Equal(tp.Topic, "mytopic");
            Assert.Equal(tp.Partition, 42);
        }

        [Fact]
        public void Equality()
        {
            var a = new TopicPartition("a", 31);
            var a2 = new TopicPartition("a", 31);
            var nes = new List<TopicPartition> {
                new TopicPartition("b", 31),
                new TopicPartition("a", 32),
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
            var tp = new TopicPartition("mytopic", 42);
            Assert.True(tp.ToString().Contains(tp.Topic));
            Assert.True(tp.ToString().Contains(tp.Partition.ToString()));
        }
    }
}
