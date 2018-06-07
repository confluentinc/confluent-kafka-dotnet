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
using System.Threading;
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
        ///     Test functionality of AdminClient.CreatePartitions
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void AdminClient_CreatePartitions(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            var topicName1 = Guid.NewGuid().ToString();
            var topicName2 = Guid.NewGuid().ToString();
            var topicName3 = Guid.NewGuid().ToString();

            // test creating a new partition works.
            using (var producer = new Producer<Null, Null>(new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } }, null, null))
            using (var adminClient = new AdminClient(producer.Handle))
            {
                var cResult = adminClient.CreateTopicsAsync(new NewTopic[] { new NewTopic { Name = topicName1, NumPartitions = 1, ReplicationFactor = 1 } }).Result;
                Assert.Single(cResult);
                Assert.False(cResult.First().Error.IsError);
                
                var cpResult = adminClient.CreatePartitionsAsync(new List<NewPartitions> { new NewPartitions { Topic = topicName1, IncreaseTo = 2 } }).Result;
                Assert.Single(cpResult);
                Assert.False(cpResult.First().Error.IsError);
                Assert.Equal(topicName1, cpResult.First().Topic);

                var dr1 = producer.ProduceAsync(new TopicPartition(topicName1, 0), new Message<Null, Null> {}).Result;
                var dr2 = producer.ProduceAsync(new TopicPartition(topicName1, 1), new Message<Null, Null> {}).Result;
                Assert.False(dr1.Error.IsError);
                Assert.False(dr2.Error.IsError);
                
                try
                {
                    producer.ProduceAsync(new TopicPartition(topicName1, 2), new Message<Null, Null> {}).Wait();
                    Assert.True(false, "expecting exception");
                }
                catch (KafkaException ex)
                {
                    Assert.True(ex.Error.IsError);
                }
            }

            // check validate only works.
            using (var producer = new Producer<Null, Null>(new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } }, null, null))
            using (var adminClient = new AdminClient(producer.Handle))
            {
                adminClient.CreateTopicsAsync(new NewTopic[] { new NewTopic { Name = topicName2, NumPartitions = 1, ReplicationFactor = 1 } }).Wait();
                var cpResult = adminClient.CreatePartitionsAsync(new List<NewPartitions> { new NewPartitions { Topic = topicName2, IncreaseTo = 10 } }, new CreatePartitionsOptions { ValidateOnly = true }).Result;

                try
                {
                    var r = producer.ProduceAsync(new TopicPartition(topicName2, 1), new Message<Null, Null> {}).Result;
                    Assert.True(false, "expecting exception");
                }
                catch (KafkaException ex)
                {
                    Assert.True(ex.Error.IsError);
                }
            }

            // check valid Assignments works.
            using (var producer = new Producer<Null, Null>(new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } }, null, null))
            using (var adminClient = new AdminClient(producer.Handle))
            {
                adminClient.CreateTopicsAsync(new NewTopic[] { new NewTopic { Name = topicName2, NumPartitions = 1, ReplicationFactor = 1 } }).Wait();
                var cpResult = adminClient.CreatePartitionsAsync(
                    new List<NewPartitions> 
                    {
                        new NewPartitions { Topic = topicName2, IncreaseTo = 2, Assignments = new List<List<int>> { new List<int> { 0 } } } 
                    }, 
                    new CreatePartitionsOptions { ValidateOnly = true }
                ).Result;

                Assert.Single(cpResult);
                Assert.False(cpResult.First().Error.IsError);
            }

            // check invalid Assignments
            using (var producer = new Producer<Null, Null>(new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } }, null, null))
            using (var adminClient = new AdminClient(producer.Handle))
            {

            }
        }
    }
}
