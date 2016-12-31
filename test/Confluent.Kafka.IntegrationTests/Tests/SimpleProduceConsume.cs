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

            Message<Null, string> dr;
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
                consumer.Assign(new List<TopicPartitionOffset>() { dr.TopicPartitionOffset });
                Message msg;
                Assert.True(consumer.Consume(out msg, TimeSpan.FromSeconds(10)));
                Assert.Equal(testString, Encoding.UTF8.GetString(msg.Value, 0, msg.Value.Length));
                Assert.Equal(null, msg.Key);
                Assert.Equal(msg.Timestamp.Type, dr.Timestamp.Type);
                Assert.Equal(msg.Timestamp.DateTime, dr.Timestamp.DateTime);
            }
        }

    }
}
