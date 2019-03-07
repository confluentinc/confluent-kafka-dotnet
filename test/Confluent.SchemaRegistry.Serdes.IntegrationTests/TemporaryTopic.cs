// Copyright 2018 Confluent Inc.
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
using Confluent.Kafka;
using Confluent.Kafka.Admin;


namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public class TemporaryTopic : IDisposable
    {
        private string bootstrapServers;
        
        public string Name { get; set; }

        public TemporaryTopic(string bootstrapServers, int numPartitions)
        {
            this.bootstrapServers = bootstrapServers;
            this.Name = "dotnet_test_" + Guid.NewGuid().ToString();

            var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
            adminClient.CreateTopicsAsync(new List<TopicSpecification> {
                new TopicSpecification { Name = Name, NumPartitions = numPartitions, ReplicationFactor = 1 } }).Wait();
            adminClient.Dispose();
        }

        public void Dispose()
        {
            var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = this.bootstrapServers }).Build();
            adminClient.DeleteTopicsAsync(new List<string> { Name }).Wait();
            adminClient.Dispose();
        }
    }
}
