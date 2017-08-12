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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka.Serialization;
using Xunit;

namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Simple Consumer Pause / Resume test.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static async Task Consumer_Pause_Resume(string bootstrapServers, string topic, string partitionedTopic)
        {
            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", Guid.NewGuid().ToString() },
                { "bootstrap.servers", bootstrapServers },
                { "auto.offset.reset", "latest" }
            };

            var producerConfig = new Dictionary<string, object> { {"bootstrap.servers", bootstrapServers}};

            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                IEnumerable<TopicPartition> assignedPartitions = null;
                Message<Null, string> message;

                consumer.OnPartitionsAssigned += (_, partitions) =>
                {
                    consumer.Assign(partitions);
                    assignedPartitions = partitions;
                };

                consumer.Subscribe(topic);

                while (assignedPartitions == null) 
                {
                    consumer.Poll(TimeSpan.FromSeconds(1));
                }
                Assert.False(consumer.Consume(out message, TimeSpan.FromSeconds(1)));

                Assert.False(producer.ProduceAsync(topic, null, "test value").Result.Error);
                Assert.True(consumer.Consume(out message, TimeSpan.FromSeconds(30)));
                consumer.Pause(assignedPartitions);
                producer.ProduceAsync(topic, null, "test value 2").Wait();
                Assert.False(consumer.Consume(out message, TimeSpan.FromSeconds(2)));
                consumer.Resume(assignedPartitions);
                Assert.True(consumer.Consume(out message, TimeSpan.FromSeconds(10)));
            }
        }

    }
}
