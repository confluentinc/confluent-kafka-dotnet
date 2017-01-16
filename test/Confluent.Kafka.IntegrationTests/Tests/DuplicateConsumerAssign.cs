using System;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka.Serialization;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     This is an experiment to see what happens when two consumers in the
    ///     same group read from the same topic/partition.
    /// </summary>
    /// <remarks>
    ///     You should never do this, but the brokers don't actually prevent it.
    /// </remarks>
    public static partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void DuplicateConsumerAssign(string bootstrapServers, string topic)
        {
            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", "u-bute-group" },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 }
            };
            var producerConfig = new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } };

            var testString = "hello world";

            Message<Null, string> dr;
            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                dr = producer.ProduceAsync(topic, null, testString).Result;
                producer.Flush();
            }

            using (var consumer1 = new Consumer(consumerConfig))
            using (var consumer2 = new Consumer(consumerConfig))
            {
                consumer1.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(topic, dr.Partition, 0) });
                consumer2.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(topic, dr.Partition, 0) });
                Message msg;
                var haveMsg1 = consumer1.Consume(out msg, TimeSpan.FromSeconds(10));
                var haveMsg2 = consumer2.Consume(out msg, TimeSpan.FromSeconds(10));

                // NOTE: two consumers from the same group should never be assigned to the same
                // topic / partition. This 'test' is here because I was curious to see what happened
                // in practice if this did occur. Because this is not expected usage, no validation
                // has been included in this test.
            }
        }

    }
}
