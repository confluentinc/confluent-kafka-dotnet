// Copyright 2016-2017 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System.Collections.Generic;
using Xunit;

namespace Confluent.Kafka.Tests
{
    public class TopicPartitionTimestampTests
    {
        [Fact]
        public void Constuctor()
        {
            var timestamp = new Timestamp(123456789, TimestampType.CreateTime);
            var tpt = new TopicPartitionTimestamp("mytopic", 42, timestamp);
            Assert.Equal("mytopic", tpt.Topic);
            Assert.Equal((Partition)42, tpt.Partition);
            Assert.Equal(tpt.Timestamp, timestamp);
        }

        [Fact]
        public void Equality()
        {
            var timestamp1 = new Timestamp(123456789, TimestampType.CreateTime);
            var timestamp2 = new Timestamp(-123456789, TimestampType.LogAppendTime);
            var tpt1 = new TopicPartitionTimestamp("a", 31, timestamp1);
            var tpt2 = new TopicPartitionTimestamp("a", 31, timestamp1);
            var list = new List<TopicPartitionTimestamp> {
                new TopicPartitionTimestamp("b", 31, timestamp1),
                new TopicPartitionTimestamp("a", 32, timestamp1),
                new TopicPartitionTimestamp("a", 31, timestamp2),
            };

            Assert.Equal(tpt1, tpt2);
            Assert.True(tpt1.Equals(tpt2));
            Assert.True(tpt1 == tpt2);
            Assert.False(tpt1 != tpt2);

            foreach (var item in list)
            {
                Assert.NotEqual(tpt1, item);
                Assert.False(tpt1.Equals(item));
                Assert.False(tpt1 == item);
                Assert.True(tpt1 != item);
            }
        }

        [Fact]
        public void ToStringTest()
        {
            var timestamp = new Timestamp(123456789, TimestampType.CreateTime);
            var tpt = new TopicPartitionTimestamp("mytopic", 42, timestamp);
            Assert.Contains(tpt.Topic, tpt.ToString());
            Assert.Contains(tpt.Partition.ToString(), tpt.ToString());
            Assert.Contains(tpt.Timestamp.ToString(), tpt.ToString());
        }

        [Fact]
        public void Properties()
        {
            var timestamp = new Timestamp(123456789, TimestampType.CreateTime);
            var tpt = new TopicPartitionTimestamp("mytopic", 42, timestamp);
            Assert.Equal("mytopic", tpt.Topic);
            Assert.Equal((Partition)42, tpt.Partition);
            Assert.Equal(new TopicPartition("mytopic", 42), tpt.TopicPartition);
            Assert.Equal(timestamp, tpt.Timestamp);
        }
    }
}
