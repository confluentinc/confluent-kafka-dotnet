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
        [IntegrationTest]
        public static void NullVsEmpty(string bootstrapServers, string topic)
        {
            var consumerConfig = new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers }};
            var producerConfig = new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } };

            MessageInfo dr;
            using (var producer = new Producer(producerConfig))
            {
                // Assume that all these produce calls succeed.
                dr = producer.ProduceAsync(topic, null, null).Result;
                producer.ProduceAsync(topic, null, new byte[0]).Wait();
                producer.ProduceAsync(topic, new byte[0], null).Wait();
                producer.ProduceAsync(topic, new byte[0], new byte[0]).Wait();
                producer.Flush();
            }

            using (var consumer = new Consumer(consumerConfig))
            {
                consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(topic, dr.Partition, dr.Offset) });

                var result = consumer.Consume(TimeSpan.FromMinutes(1));
                Assert.True(result.HasValue);
                var message = result.Value;
                Assert.Null(message.Key);
                Assert.Null(message.Value);

                result = consumer.Consume(TimeSpan.FromMinutes(1));
                Assert.True(result.HasValue);
                message = result.Value;
                Assert.Null(message.Key);
                Assert.Equal(message.Value, new byte[0]);

                result = consumer.Consume(TimeSpan.FromMinutes(1));
                Assert.True(result.HasValue);
                message = result.Value;
                Assert.Equal(message.Key, new byte[0]);
                Assert.Null(message.Value);

                result = consumer.Consume(TimeSpan.FromMinutes(1));
                Assert.True(result.HasValue);
                message = result.Value;
                Assert.Equal(message.Key, new byte[0]);
                Assert.Equal(message.Value, new byte[0]);
            }
        }

    }
}
