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
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Serialization;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test functionality of AdminClient.CreateTopics.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void AdminClient_CreateTopics(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            var topicName1 = Guid.NewGuid().ToString();
            var topicName2 = Guid.NewGuid().ToString();
            var topicName3 = Guid.NewGuid().ToString();
            var topicName4 = Guid.NewGuid().ToString();
            var topicName5 = Guid.NewGuid().ToString();

            // test 
            //  - construction of admin client from configuration.
            //  - creation of more than one topic.
            using (var adminClient = new AdminClient(new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } }))
            {
                List<CreateTopicResult> result = adminClient.CreateTopicsAsync(
                    new NewTopic[]
                    { 
                        new NewTopic { Name = topicName1, NumPartitions = 2, ReplicationFactor = 1 },
                        new NewTopic { Name = topicName2, NumPartitions = 12, ReplicationFactor = 1 }
                    }
                ).Result;

                Assert.Equal(2, result.Count);
                Assert.False(result[0].Error.IsError);
                Assert.False(result[1].Error.IsError);
                Assert.Equal(topicName1, result[0].Topic);
                Assert.Equal(topicName2, result[1].Topic);
            }

            // test 
            //  - construction of admin client from a producer handle
            //  - creation of topic 
            //  - producing to created topics works.
            using (var producer = new Producer<Null, Null>(new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } }, null, null))
            using (var adminClient2 = new AdminClient(producer.Handle))
            {
                List<CreateTopicResult> result = adminClient2.CreateTopicsAsync(
                    new List<NewTopic> { new NewTopic { Name = topicName3, NumPartitions = 24, ReplicationFactor = 1 } }).Result;
                Assert.Single(result);
                Assert.False(result[0].Error.IsError);
                Assert.Equal(topicName3, result[0].Topic);

                var deliveryReport1 = producer.ProduceAsync(topicName1, new Message<Null, Null> {}).Result;
                var deliveryReport2 = producer.ProduceAsync(topicName2, new Message<Null, Null> {}).Result;
                var deliveryReport3 = producer.ProduceAsync(topicName3, new Message<Null, Null> {}).Result;
                
                Assert.Equal(topicName1, deliveryReport1.Topic);
                Assert.Equal(topicName2, deliveryReport2.Topic);
                Assert.Equal(topicName3, deliveryReport3.Topic);
            }

            // test
            //  - create topic with same name as existing topic
            //  - as well as another topic that does exist (and for which create should succeed).
            using (var adminClient = new AdminClient(new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } }))
            {
                try
                {
                    var result = adminClient.CreateTopicsAsync(new List<NewTopic> 
                        { 
                            new NewTopic { Name = topicName3, NumPartitions = 1, ReplicationFactor = 1 },
                            new NewTopic { Name = topicName4, NumPartitions = 1, ReplicationFactor = 1 }
                        }
                    ).Result;
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
            using (var adminClient = new AdminClient(new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } }))
            {
                var result = adminClient.CreateTopicsAsync(
                    new List<NewTopic> { new NewTopic { Name = topicName5, NumPartitions = 1, ReplicationFactor = 1 } }, 
                    new CreateTopicsOptions { ValidateOnly = true, Timeout = TimeSpan.FromSeconds(30) }
                ).Result;

                Assert.Single(result);
                Assert.False(result.First().Error.IsError);
                Assert.Equal(topicName5, result.First().Topic);

                // creating for real shouldn't throw exception.
                result = adminClient.CreateTopicsAsync(
                    new List<NewTopic> { new NewTopic { Name = topicName5, NumPartitions = 1, ReplicationFactor = 1 } }
                ).Result;

                Assert.Single(result);
                Assert.False(result.First().Error.IsError);
                Assert.Equal(topicName5, result.First().Topic);
            }

        }
    }
}
