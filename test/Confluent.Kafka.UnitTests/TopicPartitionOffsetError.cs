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

using Xunit;
using System.Collections.Generic;


namespace Confluent.Kafka.UnitTests
{
    public class TopicPartitionOffsetErrorTests
    {
        [Fact]
        public void Constuctor()
        {
            var tpoe = new TopicPartitionOffsetError("mytopic", 42, 107, ErrorCode.Local_BadMsg);

            Assert.Equal("mytopic", tpoe.Topic);
            Assert.Equal((Partition)42, tpoe.Partition);
            Assert.Equal(107, tpoe.Offset);
            Assert.Equal(tpoe.Error, new Error(ErrorCode.Local_BadMsg));
        }

        [Fact]
        public void Equality()
        {
            var a = new TopicPartitionOffsetError("a", 31, 55, ErrorCode.NoError);
            var a2 = new TopicPartitionOffsetError("a", 31, 55, ErrorCode.NoError);
            var nes = new List<TopicPartitionOffsetError> {
                new TopicPartitionOffsetError("b", 31, 55, ErrorCode.NoError),
                new TopicPartitionOffsetError("a", 32, 55, ErrorCode.NoError),
                new TopicPartitionOffsetError("a", 31, 56, ErrorCode.NoError),
                new TopicPartitionOffsetError("a", 31, 55, ErrorCode.Local_Conflict),
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
        public void NullEquality()
        {
            var tpoe1 = new TopicPartitionOffsetError("a", 31, 55, ErrorCode.NoError);
            TopicPartitionOffsetError tpoe2 = null;
            TopicPartitionOffsetError tpoe3 = null;

            Assert.NotEqual(tpoe1, tpoe2);
            Assert.False(tpoe1.Equals(tpoe2));
            Assert.False(tpoe1 == tpoe2);
            Assert.True(tpoe1 != tpoe2);

            Assert.NotEqual(tpoe2, tpoe1);
            Assert.False(tpoe2 == tpoe1);
            Assert.True(tpoe2 != tpoe1);

            Assert.Equal(tpoe2, tpoe3);
            Assert.True(tpoe2 == tpoe3);
            Assert.False(tpoe2 != tpoe3);
        }

        [Fact]
        public void ToStringTest()
        {
            var tpoe = new TopicPartitionOffsetError("mytopic", 42, 107, ErrorCode.Local_BadMsg);

            Assert.Contains(tpoe.Topic, tpoe.ToString());
            Assert.Contains(tpoe.Partition.ToString(), tpoe.ToString());
            Assert.Contains(tpoe.Offset.ToString(), tpoe.ToString());
            Assert.Contains(tpoe.Error.ToString(), tpoe.ToString());
            Assert.Contains(tpoe.Error.Reason, tpoe.ToString());
        }

        [Fact]
        public void Properties()
        {
            var tpoe = new TopicPartitionOffsetError("mytopic", 42, 107, ErrorCode.NoError);

            Assert.Equal(tpoe.TopicPartition, new TopicPartition("mytopic", 42));
            Assert.Equal(tpoe.TopicPartitionOffset, new TopicPartitionOffset("mytopic", 42, 107));
        }

        [Fact]
        public void ExplicitCast()
        {
            var tpoe = new TopicPartitionOffsetError("mytopic", 42, 107, ErrorCode.NoError);
            var tpo = (TopicPartitionOffset) tpoe;
            Assert.Equal(tpoe.TopicPartitionOffset, tpo);

            tpoe = new TopicPartitionOffsetError("mytopic", 42, 107, ErrorCode.Local_BadMsg);
            Assert.Throws<KafkaException>(() => (TopicPartitionOffset) tpoe);
        }
    }
}
