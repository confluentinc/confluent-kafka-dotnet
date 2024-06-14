using System;
using System.Threading;
using Xunit;

namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///  Validates consumer doesn't freeze from log handler when MaxPollInterval exceeded
        ///  and exceptions are thrown from later poll.
        /// </summary>
        [Theory(Timeout=30000), MemberData(nameof(KafkaParameters))]
        public void Consumer_MaxPollInterval(string bootstrapServers)
        {
            LogToFile("start Consumer_MaxPollInterval");

            var sessionTimeoutMs = TimeSpan.FromSeconds(6);
            var maxPollIntervalMs = sessionTimeoutMs.Add(TimeSpan.FromMilliseconds(1));

            Util.ProduceNullStringMessages(bootstrapServers, singlePartitionTopic, 100, 2);

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = Guid.NewGuid().ToString(),
                MaxPollIntervalMs = (int)maxPollIntervalMs.TotalMilliseconds,
                SessionTimeoutMs = (int)sessionTimeoutMs.TotalMilliseconds,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var maxPolled = 0;

            using (var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig)
                   .SetLogHandler(((consumerWithin, message) =>
                   {
                       if (message.Facility.Equals("MAXPOLL"))
                       {
                           LogToFile("Max poll interval exceeded");
                           maxPolled = 1;
                           // This should not freeze
                           var memberId = consumerWithin.MemberId;
                       }
                   }))
                   .Build())
            {
                consumer.Subscribe(singlePartitionTopic);
                var count = 0;
                while (true)
                {
                    if(count == 0)
                    {
                        var cr = consumer.Consume(TimeSpan.FromSeconds(10));
                        if (cr == null) { continue; }
                        if (count == 0)
                        {
                            Thread.Sleep(maxPollIntervalMs.Add(TimeSpan.FromSeconds(5)));
                        }
                    } else
                    {
                        Assert.Throws<ConsumeException>(() => consumer.Consume(TimeSpan.FromSeconds(10)));
                        break;
                    }
                }
            }

            Assert.Equal(1, maxPolled);
            Assert.Equal(0, Library.HandleCount);
            LogToFile("end Consumer_MaxPollInterval");
        }
    }
}
