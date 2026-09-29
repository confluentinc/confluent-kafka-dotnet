// Copyright 2026 Confluent Inc.
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
    public class TypeForwardsTests
    {
        private static readonly Type[] ForwardedTypes =
        {
            typeof(ConsumeResult<,>),
            typeof(IConsumerGroupMetadata),
            typeof(DeliveryReport<,>),
            typeof(DeliveryResult<,>),
            typeof(Error),
            typeof(ErrorCode),
            typeof(ErrorCodeExtensions),
            typeof(Handle),
            typeof(Header),
            typeof(Headers),
            typeof(IClient),
            typeof(IConsumer<,>),
            typeof(IHeader),
            typeof(IProducer<,>),
            typeof(KafkaException),
            typeof(Message<,>),
            typeof(MessageMetadata),
            typeof(MessageNullException),
            typeof(Offset),
            typeof(Partition),
            typeof(PersistenceStatus),
            typeof(Timestamp),
            typeof(TimestampType),
            typeof(TopicPartition),
            typeof(TopicPartitionOffset),
            typeof(TopicPartitionOffsetError),
            typeof(TopicPartitionTimestamp),
            typeof(WatermarkOffsets),
            typeof(Config),
            typeof(ConfigPropertyNames),
            typeof(IAsyncSerializer<>),
            typeof(IAsyncDeserializer<>),
            typeof(MessageComponentType),
            typeof(SerializationContext)
        };

        [Fact]
        public void MovedPublicTypes_AreForwardedAndResolveThroughConfluentKafkaAssembly()
        {
            var kafkaAssembly = typeof(Producer<,>).Assembly;
            var forwardedTypes = kafkaAssembly.GetForwardedTypes();

            Assert.All(forwardedTypes, type => Assert.Same(typeof(Error).Assembly, type.Assembly));

            foreach (var type in ForwardedTypes)
            {
                Assert.Contains(type, forwardedTypes);
                Assert.Same(type, kafkaAssembly.GetType(type.FullName, throwOnError: true));
                Assert.Same(typeof(Error).Assembly, type.Assembly);
            }
        }
    }
}
