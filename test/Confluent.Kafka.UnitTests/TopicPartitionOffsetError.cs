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


namespace Confluent.Kafka.Tests
{
    public class TopicPartitionOffsetErrorTests
    {
        [Fact]
        public void Constuctor()
        {
            var tpoe = new TopicPartitionOffsetError("mytopic", 42, 107, ErrorCode._BAD_MSG);

            Assert.Equal(tpoe.Topic, "mytopic");
            Assert.Equal(tpoe.Partition, 42);
            Assert.Equal(tpoe.Offset, 107);
            Assert.Equal(tpoe.Error, new Error(ErrorCode._BAD_MSG));
        }

        [Fact]
        public void Equality()
        {
            var a = new TopicPartitionOffsetError("a", 31, 55, ErrorCode.NO_ERROR);
            var a2 = new TopicPartitionOffsetError("a", 31, 55, ErrorCode.NO_ERROR);
            var nes = new List<TopicPartitionOffsetError> {
                new TopicPartitionOffsetError("b", 31, 55, ErrorCode.NO_ERROR),
                new TopicPartitionOffsetError("a", 32, 55, ErrorCode.NO_ERROR),
                new TopicPartitionOffsetError("a", 31, 56, ErrorCode.NO_ERROR),
                new TopicPartitionOffsetError("a", 31, 55, ErrorCode._CONFLICT),
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
            var tpoe = new TopicPartitionOffsetError("mytopic", 42, 107, ErrorCode._BAD_MSG);

            Assert.True(tpoe.ToString().Contains(tpoe.Topic));
            Assert.True(tpoe.ToString().Contains(tpoe.Partition.ToString()));
            Assert.True(tpoe.ToString().Contains(tpoe.Offset.ToString()));
            Assert.True(tpoe.ToString().Contains(((int)tpoe.Error.Code).ToString()));
            Assert.True(tpoe.ToString().Contains(tpoe.Error.Message));
        }

        [Fact]
        public void Properties()
        {
            var tpoe = new TopicPartitionOffsetError("mytopic", 42, 107, ErrorCode.NO_ERROR);

            Assert.Equal(tpoe.TopicPartition, new TopicPartition("mytopic", 42));
            Assert.Equal(tpoe.TopicPartitionOffset, new TopicPartitionOffset("mytopic", 42, 107));
        }
    }
}
