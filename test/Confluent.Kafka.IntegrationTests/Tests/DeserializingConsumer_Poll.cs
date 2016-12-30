using System;
using System.Linq;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka.Serialization;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Basic DeserializingConsumer test (poll mode).
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void DeserializingConsumer_Poll(string bootstrapServers, string topic)
        {
            int N = 2;
            var firstProduced = Util.ProduceMessages(bootstrapServers, topic, 100, N);

            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", "simple-produce-consume" },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 }
            };

            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                int msgCnt = 0;
                bool done = false;

                consumer.OnMessage += (_, msg) =>
                {
                    Assert.Equal(msg.Error.Code, ErrorCode.NO_ERROR);
                    msgCnt += 1;
                };

                consumer.OnPartitionEOF += (_, partition)
                    => done = true;

                consumer.OnPartitionsAssigned += (_, partitions) =>
                {
                    Assert.Equal(partitions.Count, 1);
                    Assert.Equal(partitions[0], firstProduced.TopicPartition);
                    consumer.Assign(partitions.Select(p => new TopicPartitionOffset(p, firstProduced.Offset)));
                };

                consumer.OnPartitionsRevoked += (_, partitions)
                    => consumer.Unassign();

                consumer.Subscribe(topic);

                while (!done)
                {
                    consumer.Poll(TimeSpan.FromMilliseconds(100));
                }

                Assert.Equal(msgCnt, N);
            }
        }

    }
}
