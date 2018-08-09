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
        ///     Test functionality of AdminClient.CreateTopics.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void AdminClient_DeleteTopics(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start AdminClient_DeleteTopics");

            var topicName1 = Guid.NewGuid().ToString();
            var topicName2 = Guid.NewGuid().ToString();
            var topicName3 = Guid.NewGuid().ToString();
            
            // test single delete topic.
            using (var adminClient = new AdminClient(new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } }))
            {
                var cResult = adminClient.CreateTopicsAsync(
                    new List<NewTopic> { new NewTopic { Name = topicName1, NumPartitions = 1, ReplicationFactor = 1 } }).Result;
                Thread.Sleep(TimeSpan.FromSeconds(1));

                Assert.Single(cResult);
                Assert.False(cResult.First().Error.IsError);

                Thread.Sleep(TimeSpan.FromSeconds(2)); // git the topic some time to be created.
                var dResult = adminClient.DeleteTopicsAsync(new List<string> { topicName1 }).Result;

                Assert.Single(dResult);
                Assert.False(dResult.First().Error.IsError);
                Assert.Equal(topicName1, dResult.First().Topic);
            }

            // test
            //  - delete two topics, one that doesn't exist.
            //  - check that explicitly giving options doesn't obviously not work.
            using (var adminClient = new AdminClient(new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } }))
            {
                var cResult = adminClient.CreateTopicsAsync(
                    new List<NewTopic> { new NewTopic { Name = topicName2, NumPartitions = 1, ReplicationFactor = 1 } }).Result;
                Thread.Sleep(TimeSpan.FromSeconds(1));

                Assert.Single(cResult);
                Assert.False(cResult.First().Error.IsError);

                Thread.Sleep(TimeSpan.FromSeconds(2));
                try
                {
                    var dResult = adminClient.DeleteTopicsAsync(
                        new List<string> { topicName2, topicName3 },
                        new DeleteTopicsOptions { Timeout = TimeSpan.FromSeconds(30) }
                    ).Result;
                }
                catch (AggregateException ex)
                {
                    var dte = (DeleteTopicsException) ex.InnerException;
                    Assert.Equal(2, dte.Results.Count);
                    Assert.Single(dte.Results.Where(r => r.Error.IsError));
                    Assert.Single(dte.Results.Where(r => !r.Error.IsError));
                    Assert.Equal(topicName2, dte.Results.Where(r => !r.Error.IsError).First().Topic);
                    Assert.Equal(topicName3, dte.Results.Where(r => r.Error.IsError).First().Topic);
                }
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   AdminClient_DeleteTopics");
        }
    }
}
