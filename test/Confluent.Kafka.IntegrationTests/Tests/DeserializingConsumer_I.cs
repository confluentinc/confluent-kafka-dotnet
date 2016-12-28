using System.Linq;
using System.Text;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka.Serialization;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Basic DeserializingConsumer test (background poll mode).
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void DeserializingConsumer_I(string bootstrapServers, string topic)
        {
            int N = 2;
            var firstProducedMessage = Util.ProduceMessages(bootstrapServers, topic, 100, N);

            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", "simple-produce-consume" },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 }
            };

            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                int msgCnt = 0;
                AutoResetEvent are = new AutoResetEvent(false);

                consumer.OnMessage += (_, msg) =>
                {
                    Assert.Equal(msg.Error.Code, ErrorCode.NO_ERROR);
                    msgCnt += 1;
                };

                consumer.OnPartitionEOF += (_, partition)
                    => are.Set();

                consumer.OnPartitionsAssigned += (_, partitions)
                    => consumer.Assign(partitions.Select(p => new TopicPartitionOffset(p, firstProducedMessage)));

                consumer.OnPartitionsRevoked += (_, partitions)
                    => consumer.Unassign();

                consumer.Subscribe(topic);

                consumer.Start();
                are.WaitOne();
                consumer.Stop();

                Assert.Equal(msgCnt, N);
            }
        }

    }
}
