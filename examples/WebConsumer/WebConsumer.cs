using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace WebConsumer
{
    public class WebConsumer
    {
        private readonly static string _BOOTSRAP_SERVER = "localhost:9092"; // BROKER address
        private readonly static string _TOPIC = "YourTopicName"; // Topic name
        private readonly static string _GroupId = "GroupName";

        static ConsumerConfig config;
        static IConsumer<string, string> consumer;

        public WebConsumer()
        {
            config = new ConsumerConfig
            {
                GroupId = _GroupId,
                BootstrapServers = _BOOTSRAP_SERVER,
                //Latest brings only the latest data, Earliest to bring all data from the topic.
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = true,
                AutoCommitIntervalMs = 1000,
                FetchMaxBytes = 7340032,
                SessionTimeoutMs = 30000
            };
            consumer = new ConsumerBuilder<string, string>(config).Build();
        }

        public async Task Start()
        {
            try
            {
                consumer.Subscribe(_TOPIC);
                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                var consumeResult = consumer.Consume(cts.Token);
                Debug.WriteLine($"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

            }
            catch (ConsumeException ex)
            {
                Debug.WriteLine($"Consume error: {ex.Error.Reason}");
            }
        }

    }
}
