// Copyright 2024 Confluent Inc.
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
using System.Collections.Generic;
using Xunit;
using Moq;


namespace Confluent.Kafka.UnitTests
{
    public class StoreOffsetsTests
    {
        /// <summary>
        ///     Test that StoreOffsets can be mocked via the IConsumer interface.
        /// </summary>
        [Fact]
        public void MockStoreOffsets()
        {
            var mock = new Mock<IConsumer<string, string>>();

            var storedOffsets = new List<TopicPartitionOffset>();
            mock.Setup(c => c.StoreOffsets(It.IsAny<IEnumerable<TopicPartitionOffset>>()))
                .Callback<IEnumerable<TopicPartitionOffset>>(offsets => storedOffsets.AddRange(offsets));

            var consumer = mock.Object;
            var offsets = new List<TopicPartitionOffset>
            {
                new TopicPartitionOffset("topic-a", 0, 10),
                new TopicPartitionOffset("topic-a", 1, 20),
                new TopicPartitionOffset("topic-b", 0, 30)
            };

            consumer.StoreOffsets(offsets);

            mock.Verify(c => c.StoreOffsets(It.IsAny<IEnumerable<TopicPartitionOffset>>()), Times.Once);
            Assert.Equal(3, storedOffsets.Count);
            Assert.Equal("topic-a", storedOffsets[0].Topic);
            Assert.Equal(0, storedOffsets[0].Partition.Value);
            Assert.Equal(10, storedOffsets[0].Offset.Value);
            Assert.Equal("topic-a", storedOffsets[1].Topic);
            Assert.Equal(1, storedOffsets[1].Partition.Value);
            Assert.Equal(20, storedOffsets[1].Offset.Value);
            Assert.Equal("topic-b", storedOffsets[2].Topic);
            Assert.Equal(0, storedOffsets[2].Partition.Value);
            Assert.Equal(30, storedOffsets[2].Offset.Value);
        }

        /// <summary>
        ///     Test that StoreOffsets mock can throw TopicPartitionOffsetException.
        /// </summary>
        [Fact]
        public void MockStoreOffsets_ThrowsTopicPartitionOffsetException()
        {
            var mock = new Mock<IConsumer<string, string>>();

            var errorResults = new List<TopicPartitionOffsetError>
            {
                new TopicPartitionOffsetError("topic-a", 0, 10, new Error(ErrorCode.Local_State))
            };

            mock.Setup(c => c.StoreOffsets(It.IsAny<IEnumerable<TopicPartitionOffset>>()))
                .Throws(new TopicPartitionOffsetException(errorResults));

            var consumer = mock.Object;
            var offsets = new List<TopicPartitionOffset>
            {
                new TopicPartitionOffset("topic-a", 0, 10)
            };

            var ex = Assert.Throws<TopicPartitionOffsetException>(() => consumer.StoreOffsets(offsets));
            Assert.Single(ex.Results);
            Assert.Equal(ErrorCode.Local_State, ex.Results[0].Error.Code);
        }

        /// <summary>
        ///     Test that StoreOffsets on unassigned partition throws
        ///     TopicPartitionOffsetException.
        /// </summary>
        [Fact]
        public void StoreOffsets_UnassignedPartition()
        {
            using (var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
                {
                    BootstrapServers = "localhost:666",
                    GroupId = Guid.NewGuid().ToString(),
                    EnableAutoOffsetStore = false
                }).Build())
            {
                var ex = Assert.Throws<KafkaException>(() =>
                    consumer.StoreOffsets(new List<TopicPartitionOffset>
                    {
                        new TopicPartitionOffset("nonexistent-topic", 0, 1)
                    }));
                Assert.Equal(ErrorCode.Local_UnknownPartition, ex.Error.Code);
            }
        }
    }
}
