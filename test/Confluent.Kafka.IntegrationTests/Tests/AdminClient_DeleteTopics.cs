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
using System.Threading;
using System.Threading.Tasks;
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
        public async Task AdminClient_DeleteTopics(string bootstrapServers)
        {
            LogToFile("start AdminClient_DeleteTopics");

            var topicName1 = Guid.NewGuid().ToString();
            var topicName2 = Guid.NewGuid().ToString();
            var topicName3 = Guid.NewGuid().ToString();
            
            // test single delete topic.
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                await adminClient.CreateTopicsAsync(
                    new List<TopicSpecification> { new TopicSpecification { Name = topicName1, NumPartitions = 1, ReplicationFactor = 1 } });
                Thread.Sleep(TimeSpan.FromSeconds(1));

                Thread.Sleep(TimeSpan.FromSeconds(2)); // git the topic some time to be created.
                await adminClient.DeleteTopicsAsync(new List<string> { topicName1 });
            }

            // test
            //  - delete two topics, one that doesn't exist.
            //  - check that explicitly giving options doesn't obviously not work.
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                await adminClient.CreateTopicsAsync(
                    new List<TopicSpecification> { new TopicSpecification { Name = topicName2, NumPartitions = 1, ReplicationFactor = 1 } });
                Thread.Sleep(TimeSpan.FromSeconds(1));

                Thread.Sleep(TimeSpan.FromSeconds(2));
                try
                {
                    await adminClient.DeleteTopicsAsync(
                        new List<string> { topicName2, topicName3 },
                        new DeleteTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(30) }
                    );
                }
                catch (DeleteTopicsException dte)
                {
                    Assert.Equal(2, dte.Results.Count);
                    Assert.Single(dte.Results, r => r.Error.IsError);
                    Assert.Single(dte.Results, r => !r.Error.IsError);
                    Assert.Equal(topicName2, dte.Results.Where(r => !r.Error.IsError).First().Topic);
                    Assert.Equal(topicName3, dte.Results.Where(r => r.Error.IsError).First().Topic);
                }
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   AdminClient_DeleteTopics");
        }
    }
}
