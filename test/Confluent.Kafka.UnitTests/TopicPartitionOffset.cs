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
    public class TopicPartitionOffsetTests
    {
        [Fact]
        public void Constuctor()
        {
            var tpo = new TopicPartitionOffset("mytopic", 42, 107);
            Assert.Equal("mytopic", tpo.Topic);
            Assert.Equal((Partition)42, tpo.Partition);
            Assert.Equal(107, tpo.Offset);
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
        public void NullEquality()
        {
            var tpo1 = new TopicPartitionOffset("a", 31, 55);
            TopicPartitionOffset tpo2 = null;
            TopicPartitionOffset tpo3 = null;

            Assert.NotEqual(tpo1, tpo2);
            Assert.False(tpo1.Equals(tpo2));
            Assert.False(tpo1 == tpo2);
            Assert.True(tpo1 != tpo2);

            Assert.NotEqual(tpo2, tpo1);
            Assert.False(tpo2 == tpo1);
            Assert.True(tpo2 != tpo1);

            Assert.Equal(tpo2, tpo3);
            Assert.True(tpo2 == tpo3);
            Assert.False(tpo2 != tpo3);
        }

        [Fact]
        public void ToStringTest()
        {
            var tpo = new TopicPartitionOffset("mytopic", 42, 107);
            Assert.Contains(tpo.Topic, tpo.ToString());
            Assert.Contains(tpo.Partition.ToString(), tpo.ToString());
            Assert.Contains(tpo.Offset.ToString(), tpo.ToString());
        }

        [Fact]
        public void Properties()
        {
            var tpo = new TopicPartitionOffset("mytopic", 42, 107);
            Assert.Equal(tpo.TopicPartition, new TopicPartition("mytopic", 42));
        }
    }
}
