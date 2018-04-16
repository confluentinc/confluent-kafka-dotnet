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
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka.Serialization;
using static Confluent.Kafka.AdminClient;
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
            using (var adminClient = new AdminClient(new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } }))
            {
                adminClient.CreateTopic(new NewTopicSpecification { Name = "mytopic", NumPartitions = 1, ReplicationFactor = 1 });
                adminClient.CreateTopics(new List<NewTopicSpecification> { new NewTopicSpecification { Name = "mytopic" } }, new CreateTopicsOptions { ValidateOnly = false });
            }
        }
    }
}
