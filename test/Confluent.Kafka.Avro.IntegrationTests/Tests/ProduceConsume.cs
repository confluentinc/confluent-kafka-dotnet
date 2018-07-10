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
        ///     Test that messages produced with the avro serializer can be consumed with the
        ///     avro deserializer.
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void ProduceConsume(string bootstrapServers, string schemaRegistryServers)
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
                for (int i = 0; i < 100; ++i)
                {
                    var user = new User
                    {
                        name = i.ToString(),
                        favorite_number = i,
                        favorite_color = "blue"
                    };
                    producer.ProduceAsync(topic, new Message<string, User> { Key = user.name, Value = user });
                }
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            using (var consumer = new Consumer<string, User>(consumerConfig, new AvroDeserializer<string>(), new AvroDeserializer<User>()))
            {
                bool done = false;
                int i = 0;
                consumer.OnRecord += (o, record) =>
                {
                    Assert.Equal(i.ToString(), record.Message.Key);
                    Assert.Equal(i.ToString(), record.Message.Value.name);
                    Assert.Equal(i, record.Message.Value.favorite_number);
                    Assert.Equal("blue", record.Message.Value.favorite_color);

                    i++;
                };

                consumer.OnError += (o, error) =>
                {
                    Assert.True(false, error.Reason);
                };

                consumer.OnPartitionEOF += (o, e)
                    => done = true;

                consumer.Subscribe(topic);

                while (!done)
                {
                    consumer.Poll(TimeSpan.FromMilliseconds(100));
                }

                Assert.Equal(100, i);
            }
        }

    }
}
