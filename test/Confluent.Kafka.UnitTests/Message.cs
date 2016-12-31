using System;
using Xunit;


namespace Confluent.Kafka.Tests
{
    public class MessageTests
    {
        [Fact]
        public void ConstuctorAndProps()
        {
            byte[] key = new byte[0];
            byte[] val = new byte[0];
            var mi = new Message("tp1", 24, 33, key, val, new Timestamp(new DateTime(2001, 3, 4), TimestampType.CreateTime), new Error(ErrorCode.NO_ERROR));

            Assert.Equal(mi.Topic, "tp1");
            Assert.Equal(mi.Partition, 24);
            Assert.Equal(mi.Offset, 33);
            Assert.Same(mi.Key, key);
            Assert.Same(mi.Value, val);
            Assert.Equal(mi.Timestamp, new Timestamp(new DateTime(2001, 3, 4), TimestampType.CreateTime));
            Assert.Equal(mi.Error, new Error(ErrorCode.NO_ERROR));
            Assert.Equal(mi.TopicPartition, new TopicPartition("tp1", 24));
            Assert.Equal(mi.TopicPartitionOffset, new TopicPartitionOffset("tp1", 24, 33));
        }

        [Fact]
        public void ConstuctorAndProps_Generic()
        {
            var mi = new Message<string, string>("tp1", 24, 33, "mykey", "myval", new Timestamp(new DateTime(2001, 3, 4), TimestampType.CreateTime), new Error(ErrorCode.NO_ERROR));

            Assert.Equal(mi.Topic, "tp1");
            Assert.Equal(mi.Partition, 24);
            Assert.Equal(mi.Offset, 33);
            Assert.Equal(mi.Key, "mykey");
            Assert.Equal(mi.Value, "myval");
            Assert.Equal(mi.Timestamp, new Timestamp(new DateTime(2001, 3, 4), TimestampType.CreateTime));
            Assert.Equal(mi.Error, new Error(ErrorCode.NO_ERROR));
            Assert.Equal(mi.TopicPartition, new TopicPartition("tp1", 24));
            Assert.Equal(mi.TopicPartitionOffset, new TopicPartitionOffset("tp1", 24, 33));
        }
    }
}
