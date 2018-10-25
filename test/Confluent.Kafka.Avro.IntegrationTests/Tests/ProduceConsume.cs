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
using System.Threading;
using Confluent.Kafka.Examples.AvroSpecific;
using Confluent.SchemaRegistry;
using Confluent.Kafka.AvroSerdes;
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

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers
            };

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = Guid.NewGuid().ToString(),
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetResetType.Earliest
            };

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                SchemaRegistryUrl = schemaRegistryServers
            };

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer = new AvroProducer(producerConfig))
            {
                producer.RegisterAvroSerializer(new AvroSerializer<string>(schemaRegistry));
                producer.RegisterAvroSerializer(new AvroSerializer<User>(schemaRegistry));

                for (int i = 0; i < 100; ++i)
                {
                    var user = new User
                    {
                        name = i.ToString(),
                        favorite_number = i,
                        favorite_color = "blue"
                    };
                    
                    producer
                        .ProduceAsync(
                            topic, new Message<string, User> { Key = user.name, Value = user },
                            SerdeType.Avro, SerdeType.Avro,
                            new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token)
                        .Wait();
                }
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var consumer = new AvroConsumer(consumerConfig))
            {
                consumer.RegisterAvroDeserializer(new AvroDeserializer<string>(schemaRegistry));
                consumer.RegisterAvroDeserializer(new AvroDeserializer<User>(schemaRegistry));

                bool consuming = true;
                consumer.OnPartitionEOF += (_, topicPartitionOffset)
                    => consuming = false;

                consumer.OnError += (_, e)
                    => Assert.True(false, e.Reason);

                consumer.Subscribe(topic);

                int i = 0;
                while (consuming)
                {
                    var record = consumer
                        .ConsumeAsync<string, User>(
                            SerdeType.Avro, SerdeType.Avro,
                            new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token)
                        .Result;

                    if (record != null)
                    {
                        Assert.Equal(i.ToString(), record.Message.Key);
                        Assert.Equal(i.ToString(), record.Message.Value.name);
                        Assert.Equal(i, record.Message.Value.favorite_number);
                        Assert.Equal("blue", record.Message.Value.favorite_color);
                        i += 1;
                    }
                }

                Assert.Equal(100, i);

                consumer.Close();
            }
        }

    }
}
