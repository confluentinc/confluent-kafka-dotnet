using System;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka.Serialization;
using Xunit;

namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test that produces a message then consumes it.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void SimpleProduceConsume(string bootstrapServers, string topic)
        {
            // This test assumes broker v0.10.0 or higher:
            // https://github.com/edenhill/librdkafka/wiki/Broker-version-compatibility

            var producerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "api.version.request", true }
            };

            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", "simple-produce-consume" },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 },
                { "api.version.request", true }
            };

            var testString = "hello world";

            MessageInfo<Null, string> dr;
            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                dr = producer.ProduceAsync(topic, null, testString).Result;
                Assert.Equal(topic, dr.Topic);
                Assert.NotEqual<long>(dr.Offset, Offset.Invalid);
                Assert.Equal(TimestampType.CreateTime, dr.Timestamp.Type);
                producer.Flush();
            }

            using (var consumer = new Consumer(consumerConfig))
            {
                consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(topic, dr.Partition, dr.Offset) });
                var result = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.True(result.HasValue);
                var message = result.Value;
                Assert.Equal(testString, Encoding.UTF8.GetString(message.Value, 0, message.Value.Length));
                Assert.Equal(null, message.Key);
                Assert.Equal(message.Timestamp.Type, dr.Timestamp.Type);
                Assert.Equal(message.Timestamp.DateTime, dr.Timestamp.DateTime);
            }
        }

    }
}
