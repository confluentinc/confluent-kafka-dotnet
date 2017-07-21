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

using System;
using Xunit;


namespace Confluent.Kafka.UnitTests
{
    public class MessageTests
    {
        [Fact]
        public void ConstuctorAndProps()
        {
            byte[] key = new byte[0];
            byte[] val = new byte[0];
            var mi = new Message("tp1", 24, 33, key, val, new Timestamp(123456789, TimestampType.CreateTime), new Error(ErrorCode.NoError));

            Assert.Equal(mi.Topic, "tp1");
            Assert.Equal(mi.Partition, 24);
            Assert.Equal(mi.Offset, 33);
            Assert.Same(mi.Key, key);
            Assert.Same(mi.Value, val);
            Assert.Equal(mi.Timestamp, new Timestamp(123456789, TimestampType.CreateTime));
            Assert.Equal(mi.Error, new Error(ErrorCode.NoError));
            Assert.Equal(mi.TopicPartition, new TopicPartition("tp1", 24));
            Assert.Equal(mi.TopicPartitionOffset, new TopicPartitionOffset("tp1", 24, 33));
        }

        [Fact]
        public void ConstuctorAndProps_Generic()
        {
            var mi = new Message<string, string>("tp1", 24, 33, "mykey", "myval", new Timestamp(123456789, TimestampType.CreateTime), new Error(ErrorCode.NoError));

            Assert.Equal(mi.Topic, "tp1");
            Assert.Equal(mi.Partition, 24);
            Assert.Equal(mi.Offset, 33);
            Assert.Equal(mi.Key, "mykey");
            Assert.Equal(mi.Value, "myval");
            Assert.Equal(mi.Timestamp, new Timestamp(123456789, TimestampType.CreateTime));
            Assert.Equal(mi.Error, new Error(ErrorCode.NoError));
            Assert.Equal(mi.TopicPartition, new TopicPartition("tp1", 24));
            Assert.Equal(mi.TopicPartitionOffset, new TopicPartitionOffset("tp1", 24, 33));
        }
    }
}
