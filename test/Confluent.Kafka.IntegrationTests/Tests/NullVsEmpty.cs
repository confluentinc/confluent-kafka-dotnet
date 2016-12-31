using System;
using System.Collections.Generic;
using Xunit;

namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test that null and byte[0] keys and values are produced / consumed
        ///     as expected.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void NullVsEmpty(string bootstrapServers, string topic)
        {
            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", "u-bute-group" },
                { "bootstrap.servers", bootstrapServers }
            };

            var producerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers }
            };

            Message dr;
            using (var producer = new Producer(producerConfig))
            {
                // Assume that all these produce calls succeed.
                dr = producer.ProduceAsync(topic, (byte[])null, null).Result;
                producer.ProduceAsync(topic, null, new byte[0]).Wait();
                producer.ProduceAsync(topic, new byte[0], null).Wait();
                producer.ProduceAsync(topic, new byte[0], new byte[0]).Wait();
                producer.Flush();
            }

            using (var consumer = new Consumer(consumerConfig))
            {
                consumer.Assign(new List<TopicPartitionOffset>() { dr.TopicPartitionOffset });

                Message msg;
                Assert.True(consumer.Consume(out msg, TimeSpan.FromMinutes(1)));
                Assert.Null(msg.Key);
                Assert.Null(msg.Value);

                Assert.True(consumer.Consume(out msg, TimeSpan.FromMinutes(1)));
                Assert.Null(msg.Key);
                Assert.Equal(msg.Value, new byte[0]);

                Assert.True(consumer.Consume(out msg, TimeSpan.FromMinutes(1)));
                Assert.Equal(msg.Key, new byte[0]);
                Assert.Null(msg.Value);

                Assert.True(consumer.Consume(out msg, TimeSpan.FromMinutes(1)));
                Assert.Equal(msg.Key, new byte[0]);
                Assert.Equal(msg.Value, new byte[0]);
            }
        }

    }
}
