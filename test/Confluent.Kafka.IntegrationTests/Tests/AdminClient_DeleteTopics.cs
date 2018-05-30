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
        public async static void AdminClient_DeleteTopics(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            using (var adminClient = new AdminClient(new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } }))
            {
                var newTopics = new List<NewTopic> { new NewTopic { Name = Guid.NewGuid().ToString(), NumPartitions = 24, ReplicationFactor = 1 } };

                List<DeleteTopicResult> result;
                try
                {
                    result = await adminClient.DeleteTopicsAsync(new List<string> { "my-topic" });
                }
                catch (CreateTopicsException ex)
                {
                    foreach (var r in ex.Results.Where(r => r.Error.HasError))
                    {
                        Console.WriteLine($"Could not delete topic {r.Topic}: {r.Error}");
                    }
                }
            }
        }
    }
}
