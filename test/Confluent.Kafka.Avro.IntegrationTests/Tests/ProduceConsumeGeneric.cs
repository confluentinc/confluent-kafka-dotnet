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

            var config = new ProducerConfig { BootstrapServers = bootstrapServers };
            var serdeProviderConfig = new AvroSerdeProviderConfig { SchemaRegistryUrl = schemaRegistryServers };

            var topic = Guid.NewGuid().ToString();

            DeliveryReport<Null, GenericRecord> dr;
            using (var serdeProvider = new AvroSerdeProvider(serdeProviderConfig))
            using (var p = new Producer<Null, GenericRecord>(config, null, serdeProvider.SerializerGenerator<GenericRecord>()))
            {
                var record = new GenericRecord(s);
                record.Add("name", "my name 2");
                record.Add("favorite_number", 44);
                record.Add("favorite_color", null);
                dr = p.ProduceAsync(topic, new Message<Null, GenericRecord> { Value = record }).Result;
            }

            // produce a specific record (to later consume back as a generic record).
            using (var serdeProvider = new AvroSerdeProvider(serdeProviderConfig))
            using (var p = new Producer<Null, User>(config, null, serdeProvider.SerializerGenerator<User>()))
            {
                var user = new User
                {
                    name = "my name 3",
                    favorite_number = 47,
                    favorite_color = "orange"
                };
                p.ProduceAsync(topic, new Message<Null, User> { Value = user }).Wait();
            }

            Assert.Null(dr.Message.Key);
            Assert.NotNull(dr.Message.Value);
            dr.Message.Value.TryGetValue("name", out object name);
            dr.Message.Value.TryGetValue("favorite_number", out object number);
            dr.Message.Value.TryGetValue("favorite_color", out object color);

            Assert.IsType<string>(name);
            Assert.IsType<int>(number);

            Assert.Equal("my name 2", name);
            Assert.Equal(44, number);
            Assert.Null(color);

            var cconfig = new ConsumerConfig { GroupId = Guid.NewGuid().ToString(), BootstrapServers = bootstrapServers };

            using (var serdeProvider = new AvroSerdeProvider(serdeProviderConfig))
            using (var consumer = new Consumer<Null, GenericRecord>(cconfig, null, serdeProvider.DeserializerGenerator<GenericRecord>()))
            {
                // consume generic record produced as a generic record.
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(topic, 0, dr.Offset) });
                var record = consumer.Consume(TimeSpan.FromSeconds(20));
                record.Message.Value.TryGetValue("name", out object msgName);
                record.Message.Value.TryGetValue("favorite_number", out object msgNumber);
                record.Message.Value.TryGetValue("favorite_color", out object msgColor);

                Assert.IsType<string>(msgName);
                Assert.IsType<int>(msgNumber);

                Assert.Equal("my name 2", msgName);
                Assert.Equal(44, msgNumber);
                Assert.Null(msgColor);

                // consume generic record produced as a specific record.
                record = consumer.Consume(TimeSpan.FromSeconds(20));
                record.Message.Value.TryGetValue("name", out msgName);
                record.Message.Value.TryGetValue("favorite_number", out msgNumber);
                record.Message.Value.TryGetValue("favorite_color", out msgColor);

                Assert.IsType<string>(msgName);
                Assert.IsType<int>(msgNumber);
                Assert.IsType<string>(msgColor);

                Assert.Equal("my name 3", msgName);
                Assert.Equal(47, msgNumber);
                Assert.Equal("orange", msgColor);
            }

            using (var serdeProvider = new AvroSerdeProvider(serdeProviderConfig))
            using (var consumer = new Consumer<Null, User>(cconfig, null, serdeProvider.DeserializerGenerator<User>()))
            {
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(topic, 0, dr.Offset) });
                var record = consumer.Consume(TimeSpan.FromSeconds(20));
                Assert.Equal("my name 2", record.Message.Value.name);
                Assert.Equal(44, record.Message.Value.favorite_number);
                Assert.Null(record.Message.Value.favorite_color);
            }
        }

    }
}
