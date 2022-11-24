// Copyright 2020 Confluent Inc.
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

using Xunit;
using System;
using Confluent.Kafka;

namespace Confluent.SchemaRegistry.Serdes.IntegrationTests.TestClasses1
{
    public class TestPoco
    {
        public int IntField { get; set; }
    }
}

namespace Confluent.SchemaRegistry.Serdes.IntegrationTests.TestClasses2
{
    public class TestPoco
    {
        public string StringField { get; set; }
    }
}


namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test Use Latest Version on when AutoRegister enabled and disabled. 
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void UseLatestVersionCheck(string bootstrapServers, string schemaRegistryServers) 
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            var schemaRegistryConfig = new SchemaRegistryConfig { Url = schemaRegistryServers };

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig)) 
            {
                using (var producer =
                    new ProducerBuilder<string, Confluent.SchemaRegistry.Serdes.IntegrationTests.TestClasses1.TestPoco>(producerConfig)
                        .SetValueSerializer(new JsonSerializer<Confluent.SchemaRegistry.Serdes.IntegrationTests.TestClasses1.TestPoco>(schemaRegistry))
                        .Build())
                {
                    var c = new Confluent.SchemaRegistry.Serdes.IntegrationTests.TestClasses1.TestPoco { IntField = 1 };
                    producer.ProduceAsync(topic.Name, new Message<string, Confluent.SchemaRegistry.Serdes.IntegrationTests.TestClasses1.TestPoco> { Key = "test1", Value = c }).Wait();
                }

                using (var producer = 
                    new ProducerBuilder<string, Confluent.SchemaRegistry.Serdes.IntegrationTests.TestClasses2.TestPoco>(producerConfig)
                        .SetValueSerializer(new JsonSerializer<Confluent.SchemaRegistry.Serdes.IntegrationTests.TestClasses2.TestPoco>(
                            schemaRegistry, new JsonSerializerConfig { UseLatestVersion = true, AutoRegisterSchemas = false, LatestCompatibilityStrict = true }))
                        .Build())
                {
                    var c = new Confluent.SchemaRegistry.Serdes.IntegrationTests.TestClasses2.TestPoco { StringField = "Test" };
                    Assert.Throws<AggregateException>(
                        () => producer.ProduceAsync(topic.Name, new Message<string, Confluent.SchemaRegistry.Serdes.IntegrationTests.TestClasses2.TestPoco> { Key = "test1", Value = c }).Wait());
                }
            }
        }
    }
}
