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

#pragma warning disable xUnit1026

using System;
using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Simple Consumer Pause / Resume test.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_Pause_Resume(string bootstrapServers)
        {
            LogToFile("start Consumer_Pause_Resume");

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Latest
            };

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            IEnumerable<TopicPartition> assignment = null;

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            using (var producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build())
            using (var consumer =
                new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                    .SetPartitionsAssignedHandler((c, partitions) => { assignment = partitions; })
                    .Build())
            {
                consumer.Subscribe(topic.Name);

                while (assignment == null)
                {
                    consumer.Consume(TimeSpan.FromSeconds(2));
                }

                ConsumeResult<byte[], byte[]> record = consumer.Consume(TimeSpan.FromSeconds(2));
                Assert.Null(record);

                producer.ProduceAsync(topic.Name, new Message<byte[], byte[]> { Value = Serializers.Utf8.Serialize("test value", SerializationContext.Empty) }).Wait();
                record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record?.Message);
                Assert.Equal(0, record?.Offset);

                consumer.Pause(assignment);
                producer.ProduceAsync(topic.Name, new Message<byte[], byte[]> { Value = Serializers.Utf8.Serialize("test value 2", SerializationContext.Empty) }).Wait();
                record = consumer.Consume(TimeSpan.FromSeconds(2));
                Assert.Null(record);
                consumer.Resume(assignment);
                record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record?.Message);
                Assert.Equal(1, record?.Offset);

                // check that these don't throw.
                consumer.Pause(new List<TopicPartition>());
                consumer.Resume(new List<TopicPartition>());

                consumer.Close();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_Pause_Resume");
        }

    }
}
