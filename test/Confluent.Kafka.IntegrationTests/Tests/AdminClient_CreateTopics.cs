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
using System.Linq;
using System.Collections.Generic;
using Confluent.Kafka.Admin;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test functionality of AdminClient.CreateTopics.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void AdminClient_CreateTopics(string bootstrapServers)
        {
            LogToFile("start AdminClient_CreateTopics");

            var topicName1 = Guid.NewGuid().ToString();
            var topicName2 = Guid.NewGuid().ToString();
            var topicName3 = Guid.NewGuid().ToString();
            var topicName4 = Guid.NewGuid().ToString();
            var topicName5 = Guid.NewGuid().ToString();

            // test 
            //  - construction of admin client from configuration.
            //  - creation of more than one topic.
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                adminClient.CreateTopicsAsync(
                    new TopicSpecification[]
                    { 
                        new TopicSpecification { Name = topicName1, NumPartitions = 2, ReplicationFactor = 1 },
                        new TopicSpecification { Name = topicName2, NumPartitions = 12, ReplicationFactor = 1 }
                    }
                ).Wait();
            }

            // test 
            //  - construction of admin client from a producer handle
            //  - creation of topic 
            //  - producing to created topics works.
            using (var producer = new ProducerBuilder<Null, Null>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
            using (var adminClient2 = new DependentAdminClientBuilder(producer.Handle).Build())
            {
                adminClient2.CreateTopicsAsync(
                    new List<TopicSpecification> { new TopicSpecification { Name = topicName3, NumPartitions = 24, ReplicationFactor = 1 } }).Wait();

                var deliveryReport1 = producer.ProduceAsync(topicName1, new Message<Null, Null>()).Result;
                var deliveryReport2 = producer.ProduceAsync(topicName2, new Message<Null, Null>()).Result;
                var deliveryReport3 = producer.ProduceAsync(topicName3, new Message<Null, Null>()).Result;
                
                Assert.Equal(topicName1, deliveryReport1.Topic);
                Assert.Equal(topicName2, deliveryReport2.Topic);
                Assert.Equal(topicName3, deliveryReport3.Topic);
            }

            // test
            //  - create topic with same name as existing topic
            //  - as well as another topic that does exist (and for which create should succeed).
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {
                    adminClient.CreateTopicsAsync(new List<TopicSpecification> 
                        { 
                            new TopicSpecification { Name = topicName3, NumPartitions = 1, ReplicationFactor = 1 },
                            new TopicSpecification { Name = topicName4, NumPartitions = 1, ReplicationFactor = 1 }
                        }
                    ).Wait();
                    Assert.True(false, "Expect CreateTopics request to throw an exception.");
                }

                // if awaited, the CreateTopicsException is not wrapped. 
                // this is an annoyance if used synchronously, but not atypical.
                catch (AggregateException ex)
                {
                    Assert.True(ex.InnerException.GetType() == typeof(CreateTopicsException));
                    var cte = (CreateTopicsException) ex.InnerException;
                    Assert.Equal(2, cte.Results.Count);
                    Assert.Single(cte.Results.Where(r => r.Error.IsError));
                    Assert.Single(cte.Results.Where(r => !r.Error.IsError));
                    Assert.Equal(topicName3, cte.Results.Where(r => r.Error.IsError).First().Topic);
                    Assert.Equal(topicName4, cte.Results.Where(r => !r.Error.IsError).First().Topic);
                }
            }

            // test 
            //  - validate only
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                adminClient.CreateTopicsAsync(
                    new List<TopicSpecification> { new TopicSpecification { Name = topicName5, NumPartitions = 1, ReplicationFactor = 1 } }, 
                    new CreateTopicsOptions { ValidateOnly = true, RequestTimeout = TimeSpan.FromSeconds(30) }
                ).Wait();

                // creating for real shouldn't throw exception.
                adminClient.CreateTopicsAsync(
                    new List<TopicSpecification> { new TopicSpecification { Name = topicName5, NumPartitions = 1, ReplicationFactor = 1 } }
                ).Wait();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   AdminClient_CreateTopics");
        }
    }
}
