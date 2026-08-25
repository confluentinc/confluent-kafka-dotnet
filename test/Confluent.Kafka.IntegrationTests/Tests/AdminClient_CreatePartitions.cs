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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Admin;
using Xunit;
using Confluent.Kafka.TestsCommon;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test functionality of AdminClient.CreatePartitions
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task AdminClient_CreatePartitions(string bootstrapServers)
        {
            LogToFile("start AdminClient_CreatePartitions");

            var topicName1 = Guid.NewGuid().ToString();
            var topicName2 = Guid.NewGuid().ToString();
            var topicName3 = Guid.NewGuid().ToString();
            var topicName4 = Guid.NewGuid().ToString();
            var topicName5 = Guid.NewGuid().ToString();
            var topicName6 = Guid.NewGuid().ToString();

            // test creating a new partition works.
            using (var producer = new TestProducerBuilder<Null, Null>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
            using (var adminClient = new DependentAdminClientBuilder(producer.Handle).Build())
            {
                await adminClient.CreateTopicsAsync(new TopicSpecification[] { new TopicSpecification { Name = topicName1, NumPartitions = 1, ReplicationFactor = 1 } });
                await adminClient.CreatePartitionsAsync(new List<PartitionsSpecification> { new PartitionsSpecification { Topic = topicName1, IncreaseTo = 2 } });

                var dr1 = await producer.ProduceAsync(new TopicPartition(topicName1, 0), new Message<Null, Null>(), TestContext.Current.CancellationToken);
                var dr2 = await producer.ProduceAsync(new TopicPartition(topicName1, 1), new Message<Null, Null>(), TestContext.Current.CancellationToken);

                try
                {
                    await producer.ProduceAsync(new TopicPartition(topicName1, 2), new Message<Null, Null>(), TestContext.Current.CancellationToken);
                    Assert.Fail("expecting exception");
                }
                catch (ProduceException<Null, Null> ex)
                {
                    Assert.True(ex.Error.IsError);
                }
            }

            // check validate only works.
            using (var producer = new TestProducerBuilder<Null, Null>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
            using (var adminClient = new DependentAdminClientBuilder(producer.Handle).Build())
            {
                await adminClient.CreateTopicsAsync(new TopicSpecification[] { new TopicSpecification { Name = topicName2, NumPartitions = 1, ReplicationFactor = 1 } });
                await adminClient.CreatePartitionsAsync(new List<PartitionsSpecification> { new PartitionsSpecification { Topic = topicName2, IncreaseTo = 10 } }, new CreatePartitionsOptions { ValidateOnly = true });

                // forces a metadata request.
                var dr1 = await producer.ProduceAsync(new TopicPartition(topicName2, 0), new Message<Null, Null>(), TestContext.Current.CancellationToken);
                try
                {
                    // since we have metadata, this throws immediately (i.e. not wrapped in AggregateException)
                    var dr2 = await producer.ProduceAsync(new TopicPartition(topicName2, 1), new Message<Null, Null>(), TestContext.Current.CancellationToken);
                    Assert.Fail("expecting exception");
                }
                catch (ProduceException<Null, Null> ex)
                {
                    Assert.True(ex.Error.IsError);
                }
            }

            // check valid Assignments property value works.
            using (var producer = new TestProducerBuilder<Null, Null>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
            using (var adminClient = new DependentAdminClientBuilder(producer.Handle).Build())
            {
                await adminClient.CreateTopicsAsync(new TopicSpecification[] { new TopicSpecification { Name = topicName3, NumPartitions = 1, ReplicationFactor = 1 } });
                await adminClient.CreatePartitionsAsync(
                    new List<PartitionsSpecification>
                    {
                        new PartitionsSpecification { Topic = topicName2, IncreaseTo = 2, ReplicaAssignments = new List<List<int>> { new List<int> { 0 } } }
                    },
                    new CreatePartitionsOptions { ValidateOnly = true }
                );
            }

            // check invalid Assignments property value works.
            using (var producer = new TestProducerBuilder<Null, Null>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
            using (var adminClient = new DependentAdminClientBuilder(producer.Handle).Build())
            {
                await adminClient.CreateTopicsAsync(new TopicSpecification[] { new TopicSpecification { Name = topicName4, NumPartitions = 1, ReplicationFactor = 1 } });

                try
                {
                    await adminClient.CreatePartitionsAsync(
                        new List<PartitionsSpecification>
                        {
                            new PartitionsSpecification { Topic = topicName2, IncreaseTo = 2, ReplicaAssignments = new List<List<int>> { new List<int> { 42 } } }
                        },
                        new CreatePartitionsOptions { ValidateOnly = true }
                    );
                    Assert.Fail("Expecting exception");
                }
                catch (CreatePartitionsException cpe)
                {
                    Assert.Single(cpe.Results);
                    Assert.True(cpe.Results.First().Error.IsError);
                }
            }

            // more than one.
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                await adminClient.CreateTopicsAsync(new TopicSpecification[]
                    {
                        new TopicSpecification { Name = topicName5, NumPartitions = 1, ReplicationFactor = 1 },
                        new TopicSpecification { Name = topicName6, NumPartitions = 1, ReplicationFactor = 1 }
                    }
                );
                Thread.Sleep(TimeSpan.FromSeconds(1));

                // just a simple check there wasn't an exception.
                await adminClient.CreatePartitionsAsync(
                    new List<PartitionsSpecification>
                    {
                        new PartitionsSpecification { Topic = topicName5, IncreaseTo = 2 },
                        new PartitionsSpecification { Topic = topicName6, IncreaseTo = 3 }
                    }
                );
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   AdminClient_CreatePartitions");
        }
    }
}
