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
    public class TopicPartitionTests
    {
        [Fact]
        public void Constuctor()
        {
            var tp = new TopicPartition("mytopic", 42);
            Assert.Equal("mytopic", tp.Topic);
            Assert.Equal((Partition)42, tp.Partition);
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
        public void NullEquality()
        {
            var tp1 = new TopicPartition("a", 31);
            TopicPartition tp2 = null;
            TopicPartition tp3 = null;

            Assert.NotEqual(tp1, tp2);
            Assert.False(tp1.Equals(tp2));
            Assert.False(tp1 == tp2);
            Assert.True(tp1 != tp2);

            Assert.NotEqual(tp2, tp1);
            Assert.False(tp2 == tp1);
            Assert.True(tp2 != tp1);

            Assert.Equal(tp2, tp3);
            Assert.True(tp2 == tp3);
            Assert.False(tp2 != tp3);
        }

        [Fact]
        public void ToStringTest()
        {
            var tp = new TopicPartition("mytopic", 42);
            Assert.Contains(tp.Topic, tp.ToString());
            Assert.Contains(tp.Partition.ToString(), tp.ToString());
        }
    }
}
