// Copyright 2023 Confluent Inc.
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
using System.Threading.Tasks;
using System.Collections.Generic;
using Confluent.Kafka.Admin;
using Xunit;

namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public async void AdminClient_ListOffsets(string bootstrapServers)
        {
            LogToFile("start AdminClient_ListOffsets");
            
            using var topic = new TemporaryTopic(bootstrapServers, 1);
            using var producer = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build();
            using var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
            
            long basetimestamp = 10000000;
            await producer.ProduceAsync(topic.Name, new Message<Null, string> { Value = "Producer Message", Timestamp = new Timestamp(basetimestamp + 100, TimestampType.CreateTime)});
            await producer.ProduceAsync(topic.Name, new Message<Null, string> { Value = "Producer Message", Timestamp = new Timestamp(basetimestamp + 400, TimestampType.CreateTime)});
            await producer.ProduceAsync(topic.Name, new Message<Null, string> { Value = "Producer Message", Timestamp = new Timestamp(basetimestamp + 250, TimestampType.CreateTime)});
            producer.Flush(new TimeSpan(0, 0, 10));
            
            var timeout = TimeSpan.FromSeconds(30);
            ListOffsetsOptions options = new ListOffsetsOptions(){RequestTimeout = timeout, IsolationLevel = IsolationLevel.ReadUncommitted};

            var testFixtures = new List<Tuple<OffsetSpec, Offset>>
            {
                Tuple.Create(OffsetSpec.Earliest(), new Offset(0)),
                Tuple.Create(OffsetSpec.Latest(), new Offset(3)),
                Tuple.Create(OffsetSpec.MaxTimestamp(), new Offset(1)),
                Tuple.Create(OffsetSpec.ForTimestamp(basetimestamp + 150), new Offset(1)),
            };
            
            foreach (var fixture in testFixtures)
            {
                var offsetSpec = fixture.Item1;
                var offset = fixture.Item2;
                var topicPartitionOffsetSpecs = new List<TopicPartitionOffsetSpec>
                {
                    new TopicPartitionOffsetSpec
                    {
                        TopicPartition = new TopicPartition(topic.Name, new Partition(0)),
                        OffsetSpec = offsetSpec
                    }
                };
                
                var listOffsetsResult = await adminClient.ListOffsetsAsync(topicPartitionOffsetSpecs, options);

                foreach (var resultInfo in listOffsetsResult.ResultInfos)
                {
                    TopicPartitionOffsetError topicPartition = resultInfo.TopicPartitionOffsetError;
                    Assert.Equal(offset, topicPartition.Offset);
                }
            }

            LogToFile("end AdminClient_ListOffsets");
        }
    }
}
