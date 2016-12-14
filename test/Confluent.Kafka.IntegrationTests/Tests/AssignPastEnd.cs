using System;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka.Serialization;
using Xunit;

namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test functionality of Consumer.Consume when assigned to offest
    ///     higher than the offset of the last message on a partition.
    /// </summary>
    public static partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void AssignPastEnd(string bootstrapServers, string topic)
        {
            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", "u-bute-group" },
                { "bootstrap.servers", bootstrapServers },
                // TODO: Confirm that setting this timeout is really necessary here.
                { "session.timeout.ms", 6000 }
            };
            var producerConfig = new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } };

            var testString = "hello world";

            MessageInfo<Null, string> dr;
            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                dr = producer.ProduceAsync(topic, null, testString).Result;
                producer.Flush();
            }

            using (var consumer = new Consumer(consumerConfig))
            {
                consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(topic, dr.Partition, dr.Offset+1) });
                var result = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.False(result.HasValue);
            }
        }

    }
}
