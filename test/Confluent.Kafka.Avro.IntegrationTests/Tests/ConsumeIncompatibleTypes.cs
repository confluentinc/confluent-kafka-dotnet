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
using Xunit;


namespace Confluent.Kafka.Avro.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test that consuming a key/value with schema incompatible with
        ///     the strongly typed consumer instance results in an appropriate
        ///     consume error event.
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void ConsumeIncompatibleTypes(string bootstrapServers, string schemaRegistryServers)
        {
            string topic = Guid.NewGuid().ToString();

            var producerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "schema.registry.url", schemaRegistryServers }
            };

            var consumerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "group.id", Guid.NewGuid().ToString() },
                { "session.timeout.ms", 6000 },
                { "auto.offset.reset", "smallest" },
                { "schema.registry.url", schemaRegistryServers }
            };

            using (var producer = new Producer<string, User>(producerConfig, new AvroSerializer<string>(), new AvroSerializer<User>()))
            {
                var user = new User
                {
                    name = "username",
                    favorite_number = 107,
                    favorite_color = "orange"
                };
                producer.ProduceAsync(topic, new Message<string, User> { Key = user.name, Value = user });
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            using (var consumer = new Consumer<User, User>(consumerConfig, new AvroDeserializer<User>(), new AvroDeserializer<User>()))
            {
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(topic, 0, 0) });

                bool hadError = false;
                try
                {
                    consumer.Consume(TimeSpan.FromSeconds(10));
                }
                catch (ConsumeException e)
                {
                    if (e.Error.Code == ErrorCode.Local_KeyDeserialization)
                    {
                        hadError = true;
                    }
                }

                Assert.True(hadError);
            }

            using (var consumer = new Consumer<string, string>(consumerConfig, new AvroDeserializer<string>(), new AvroDeserializer<string>()))
            {
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(topic, 0, 0) });

                bool hadError = false;
                try
                {
                    consumer.Consume(TimeSpan.FromSeconds(10));
                }
                catch (ConsumeException e)
                {
                    if (e.Error.Code == ErrorCode.Local_ValueDeserialization)
                    {
                        hadError = true;
                    }
                }

                Assert.True(hadError);
            }

        }
    }
}
