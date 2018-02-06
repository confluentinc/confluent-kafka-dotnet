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
    public class TopicPartitionErrorTests
    {
        [Fact]
        public void Constuctor()
        {
            var tpe = new TopicPartitionError("mytopic", 42, ErrorCode.Local_BadMsg);

            Assert.Equal("mytopic", tpe.Topic);
            Assert.Equal((Partition)42, tpe.Partition);
            Assert.Equal(new Error(ErrorCode.Local_BadMsg), tpe.Error);
        }

        [Fact]
        public void Equality()
        {
            var a = new TopicPartitionError("a", 31, ErrorCode.NoError);
            var a2 = new TopicPartitionError("a", 31, ErrorCode.NoError);
            var nes = new List<TopicPartitionError> {
                new TopicPartitionError("b", 31, ErrorCode.NoError),
                new TopicPartitionError("a", 32, ErrorCode.NoError),
                new TopicPartitionError("a", 31, ErrorCode.Local_Conflict),
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
            var tpe1 = new TopicPartitionError("a", 31, ErrorCode.NoError);
            TopicPartitionError tpe2 = null;
            TopicPartitionError tpe3 = null;

            Assert.NotEqual(tpe1, tpe2);
            Assert.False(tpe1.Equals(tpe2));
            Assert.False(tpe1 == tpe2);
            Assert.True(tpe1 != tpe2);

            Assert.NotEqual(tpe2, tpe1);
            Assert.False(tpe2 == tpe1);
            Assert.True(tpe2 != tpe1);

            Assert.Equal(tpe2, tpe3);
            Assert.True(tpe2 == tpe3);
            Assert.False(tpe2 != tpe3);
        }

        [Fact]
        public void ToStringTest()
        {
            var tpe = new TopicPartitionError("mytopic", 42, ErrorCode.Local_BadMsg);

            Assert.Contains(tpe.Topic, tpe.ToString());
            Assert.Contains(tpe.Partition.ToString(), tpe.ToString());
            Assert.Contains(tpe.Error.ToString(), tpe.ToString());
            Assert.Contains(tpe.Error.Reason, tpe.ToString());
        }

        [Fact]
        public void Properties()
        {
            var tpe = new TopicPartitionError("mytopic", 42, ErrorCode.NoError);

            Assert.Equal(tpe.TopicPartition, new TopicPartition("mytopic", 42));
        }
    }
}
