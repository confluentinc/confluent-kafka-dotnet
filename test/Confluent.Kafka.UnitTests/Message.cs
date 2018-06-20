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


namespace Confluent.Kafka.UnitTests
{
    public class MessageTests
    {
        [Fact]
        public void ConstructorAndProps()
        {
            byte[] key = new byte[0];
            byte[] val = new byte[0];
            var mi = new Message("tp1", 24, 33, key, val, new Timestamp(123456789, TimestampType.CreateTime), new Error(ErrorCode.NoError));

            Assert.Equal("tp1", mi.Topic);
            Assert.Equal(24, mi.Partition);
            Assert.Equal(33, mi.Offset);
            Assert.Same(key, mi.Key);
            Assert.Same(val, mi.Value);
            Assert.Equal(new Timestamp(123456789, TimestampType.CreateTime), mi.Timestamp);
            Assert.Equal(new Error(ErrorCode.NoError), mi.Error);
            Assert.Equal(new TopicPartition("tp1", 24), mi.TopicPartition);
            Assert.Equal(new TopicPartitionOffset("tp1", 24, 33), mi.TopicPartitionOffset);
        }

        [Fact]
        public void ConstructorAndProps_Generic()
        {
            var mi = new Message<string, string>("tp1", 24, 33, "mykey", "myval", new Timestamp(123456789, TimestampType.CreateTime), new Error(ErrorCode.NoError));

            Assert.Equal("tp1", mi.Topic);
            Assert.Equal(24, mi.Partition);
            Assert.Equal(33, mi.Offset);
            Assert.Equal("mykey", mi.Key);
            Assert.Equal("myval", mi.Value);
            Assert.Equal(new Timestamp(123456789, TimestampType.CreateTime), mi.Timestamp);
            Assert.Equal(new Error(ErrorCode.NoError), mi.Error);
            Assert.Equal(new TopicPartition("tp1", 24), mi.TopicPartition);
            Assert.Equal(new TopicPartitionOffset("tp1", 24, 33), mi.TopicPartitionOffset);
        }
    }
}
