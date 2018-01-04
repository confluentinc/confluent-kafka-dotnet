using System;
using System.Collections.Generic;
using Confluent.Kafka.Examples.AvroSpecific;
using Confluent.Kafka.Serialization;
using Xunit;


namespace Confluent.Kafka.AvroIntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test that messages produced with the avro serializer can be consumed with the
        ///     avro deserializer.
        /// </summary>
        public static void ProduceConsume(string schemaRegistryServers, string bootstrapServers)
        {
            string topic = Guid.NewGuid().ToString();

            var producerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "api.version.request", true },
                { "schema.registry.urls", schemaRegistryServers }
            };

            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", Guid.NewGuid().ToString() },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 },
                { "api.version.request", true },
                { "schema.registry.urls", schemaRegistryServers },
                { "default.topic.config", new Dictionary<string, object>()
                    {
                        { "auto.offset.reset", "smallest" }
                    }
                }
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
                    producer.ProduceAsync(topic, user.name, user);
                }
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            using (var consumer = new Consumer<string, User>(consumerConfig, new AvroDeserializer<string>(), new AvroDeserializer<User>()))
            {
                bool done = false;
                int i = 0;
                consumer.OnMessage += (o, e) =>
                {
                    Assert.Equal(i.ToString(), e.Key);
                    Assert.Equal(i.ToString(), e.Value.name);
                    Assert.Equal(i, e.Value.favorite_number);
                    Assert.Equal("blue", e.Value.favorite_color);

                    i++;
                };

                consumer.OnError += (o, e) =>
                {
                    Assert.True(false);
                };

                consumer.OnConsumeError += (o, e) =>
                {
                    Assert.True(false);
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
