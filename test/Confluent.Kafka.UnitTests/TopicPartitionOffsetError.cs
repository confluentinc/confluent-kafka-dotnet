using Xunit;
using System.Collections.Generic;


namespace Confluent.Kafka.Tests
{
    public class TopicPartitionOffsetErrorTests
    {
        [Fact]
        public void Constuctor()
        {
            var tpoe = new TopicPartitionOffsetError(
                "mytopic", 42, 107,
                new Error(ErrorCode._BAD_MSG, "Bad Message")
            );

            Assert.Equal(tpoe.Topic, "mytopic");
            Assert.Equal(tpoe.Partition, 42);
            Assert.Equal(tpoe.Offset, 107);
            Assert.Equal(tpoe.Error, new Error(ErrorCode._BAD_MSG, "Bad Message"));
        }

        [Fact]
        public void Equality()
        {
            var a = new TopicPartitionOffsetError("a", 31, 55, new Error(ErrorCode.NO_ERROR, null));
            var a2 = new TopicPartitionOffsetError("a", 31, 55, new Error(ErrorCode.NO_ERROR, null));
            var nes = new List<TopicPartitionOffsetError> {
                new TopicPartitionOffsetError("b", 31, 55, new Error(ErrorCode.NO_ERROR, null)),
                new TopicPartitionOffsetError("a", 32, 55, new Error(ErrorCode.NO_ERROR, null)),
                new TopicPartitionOffsetError("a", 31, 56, new Error(ErrorCode.NO_ERROR, null)),
                new TopicPartitionOffsetError("a", 31, 55, new Error(ErrorCode._CONFLICT, null)),
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
            var tpoe = new TopicPartitionOffsetError(
                "mytopic", 42, 107,
                new Error(ErrorCode._BAD_MSG, "Bad Message")
            );

            Assert.True(tpoe.ToString().Contains(tpoe.Topic));
            Assert.True(tpoe.ToString().Contains(tpoe.Partition.ToString()));
            Assert.True(tpoe.ToString().Contains(tpoe.Offset.ToString()));
            Assert.True(tpoe.ToString().Contains(((int)tpoe.Error.Code).ToString()));
            Assert.True(tpoe.ToString().Contains(tpoe.Error.Message));
        }

        [Fact]
        public void Properties()
        {
            var tpoe = new TopicPartitionOffsetError(
                "mytopic", 42, 107,
                new Error(ErrorCode.NO_ERROR, null)
            );

            Assert.Equal(tpoe.TopicPartition, new TopicPartition("mytopic", 42));
            Assert.Equal(tpoe.TopicPartitionOffset, new TopicPartitionOffset("mytopic", 42, 107));
        }
    }
}
