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
using Confluent.Kafka.Examples.AvroSpecific;
using Confluent.Kafka.Serialization;
using Avro;
using Avro.Generic;
using Xunit;


namespace Confluent.Kafka.Avro.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test that messages produced with the avro serializer can be consumed with the
        ///     avro deserializer.
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void ProduceConsumeGeneric(string bootstrapServers, string schemaRegistryServers)
        {
            var s = (RecordSchema)Schema.Parse(
                @"{
                    ""namespace"": ""Confluent.Kafka.Examples.AvroSpecific"",
                    ""type"": ""record"",
                    ""name"": ""User"",
                    ""fields"": [
                        {""name"": ""name"", ""type"": ""string""},
                        {""name"": ""favorite_number"",  ""type"": [""int"", ""null""]},
                        {""name"": ""favorite_color"", ""type"": [""string"", ""null""]}
                    ]
                  }"
            );

            var config = new Dictionary<string, object>()
            {
                { "bootstrap.servers", bootstrapServers },
                { "schema.registry.url", schemaRegistryServers }
            };

            var topic = Guid.NewGuid().ToString();

            Message<Null, GenericRecord> dr;
            using (var p = new Producer<Null, GenericRecord>(config, null, new AvroSerializer<GenericRecord>()))
            {
                var record = new GenericRecord(s);
                record.Add("name", "my name 2");
                record.Add("favorite_number", 44);
                record.Add("favorite_color", null);
                dr = p.ProduceAsync(topic, null, record).Result;
            }

            // produce a specific record (to later consume back as a generic record).
            using (var p = new Producer<Null, User>(config, null, new AvroSerializer<User>()))
            {
                var user = new User
                {
                    name = "my name 3",
                    favorite_number = 47,
                    favorite_color = "orange"
                };
                p.ProduceAsync(topic, null, user).Wait();
            }

            Assert.Null(dr.Key);
            Assert.NotNull(dr.Value);
            dr.Value.TryGetValue("name", out object name);
            dr.Value.TryGetValue("favorite_number", out object number);
            dr.Value.TryGetValue("favorite_color", out object color);

            Assert.IsType<string>(name);
            Assert.IsType<int>(number);

            Assert.Equal("my name 2", name);
            Assert.Equal(44, number);
            Assert.Null(color);

            var cconfig = new Dictionary<string, object>()
            {
                { "group.id", Guid.NewGuid().ToString() },
                { "bootstrap.servers", bootstrapServers },
                { "schema.registry.url", schemaRegistryServers }
            };

            using (var c = new Consumer<Null, GenericRecord>(cconfig, null, new AvroDeserializer<GenericRecord>()))
            {
                // consume generic record produced as a generic record.
                c.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(topic, 0, dr.Offset) });
                c.Consume(out Message<Null, GenericRecord> msg, 20000);
                msg.Value.TryGetValue("name", out object msgName);
                msg.Value.TryGetValue("favorite_number", out object msgNumber);
                msg.Value.TryGetValue("favorite_color", out object msgColor);

                Assert.IsType<string>(msgName);
                Assert.IsType<int>(msgNumber);

                Assert.Equal("my name 2", msgName);
                Assert.Equal(44, msgNumber);
                Assert.Null(msgColor);

                // consume generic record produced as a specific record.
                c.Consume(out msg, 20000);
                msg.Value.TryGetValue("name", out msgName);
                msg.Value.TryGetValue("favorite_number", out msgNumber);
                msg.Value.TryGetValue("favorite_color", out msgColor);

                Assert.IsType<string>(msgName);
                Assert.IsType<int>(msgNumber);
                Assert.IsType<string>(msgColor);

                Assert.Equal("my name 3", msgName);
                Assert.Equal(47, msgNumber);
                Assert.Equal("orange", msgColor);
            }

            using (var c = new Consumer<Null, User>(cconfig, null, new AvroDeserializer<User>()))
            {
                c.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(topic, 0, dr.Offset) });
                c.Consume(out Message<Null, User> msg, 20000);
                Assert.Equal("my name 2", msg.Value.name);
                Assert.Equal(44, msg.Value.favorite_number);
                Assert.Null(msg.Value.favorite_color);
            }
        }

    }
}
