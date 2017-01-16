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
        ///     Test that a specific user specified timestamp on produce is consumed.
        /// </summary>
        [Theory (Skip="currently no rd_kafka_producev in referenced librdkafka"), MemberData(nameof(KafkaParameters))]
        public static void ProduceTimestamp(string bootstrapServers, string topic)
        {
            var producerConfig = new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } };

            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", "simple-produce-consume" },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 },
                { "api.version.request", true }
            };

            var testString = "hello world";
            var testTime = new DateTime(2010, 1, 1, 0, 0, 0);

            var s = Library.VersionString;
            Message<Null, string> dr;
            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                dr = producer.ProduceAsync(topic, null, testString, testTime).Result;
                Assert.Equal(topic, dr.Topic);
                Assert.NotEqual<long>(dr.Offset, Offset.Invalid);
                Assert.Equal(dr.Timestamp.DateTime, testTime);
                Assert.True(false); // TODO: check timestamp type. what should it be?
                producer.Flush();
            }

            using (var consumer = new Consumer(consumerConfig))
            {
                consumer.Assign(new List<TopicPartitionOffset>() { dr.TopicPartitionOffset });
                Message msg;
                Assert.True(consumer.Consume(out msg, TimeSpan.FromSeconds(10)));
                Assert.Equal(msg.Timestamp.DateTime, testTime);
                Assert.True(false); // TODO: check timestamp type. what should it be?
            }
        }

    }
}
