using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;

namespace Confluent.Kafka.AdvancedConsumer
{
    public class Program
    {
        public static void Run(string brokerList, List<string> topics)
        {
            bool enableAutoCommit = false;

            var config = new Config()
            {
                GroupId = "advanced-csharp-consumer",
                EnableAutoCommit = enableAutoCommit,
                StatisticsInterval = TimeSpan.FromSeconds(60)
            };

            using (var consumer = new EventConsumer(config, brokerList))
            {
                consumer.OnMessage += (obj, msg) => {
                    string text = Encoding.UTF8.GetString(msg.Payload, 0, msg.Payload.Length);
                    Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {text}");

                    if (!enableAutoCommit && msg.Offset % 10 == 0)
                    {
                        Console.WriteLine($"Committing offset");
                        consumer.Commit(msg).Wait();
                        Console.WriteLine($"Committed offset");
                    }
                };

                consumer.OnConsumerError += (obj, errorCode) =>
                {
                    Console.WriteLine($"Consumer Error: {errorCode}");
                };

                consumer.OnEndReached += (obj, end) => {
                    Console.WriteLine($"Reached end of topic {end.Topic} partition {end.Partition}, next message will be at offset {end.Offset}");
                };

                consumer.OnError += (obj, error) => {
                    Console.WriteLine($"Error: {error.ErrorCode} {error.Reason}");
                };

                if (enableAutoCommit)
                {
                    consumer.OnOffsetCommit += (obj, commit) => {
                        if (commit.Error != ErrorCode.NO_ERROR)
                        {
                            Console.WriteLine($"Failed to commit offsets: {commit.Error}");
                        }
                        Console.WriteLine($"Successfully committed offsets: [{string.Join(", ", commit.Offsets)}]");
                    };
                }

                consumer.OnPartitionsAssigned += (obj, partitions) => {
                    Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}], member id: {consumer.MemberId}");
                    consumer.Assign(partitions);
                };

                consumer.OnPartitionsRevoked += (obj, partitions) => {
                    Console.WriteLine($"Revoked partitions: [{string.Join(", ", partitions)}]");
                    consumer.Unassign();
                };

                consumer.OnStatistics += (obj, json) => {
                    Console.WriteLine($"Statistics: {json}");
                };

                consumer.Subscribe(topics);
                consumer.Start();

                Console.WriteLine($"Assigned to: [{string.Join(", ", consumer.Assignment)}]");
                Console.WriteLine($"Subscribed to: [{string.Join(", ", consumer.Subscription)}]");

                Console.WriteLine($"Started consumer, press enter to stop consuming");
                Console.ReadLine();
            }
        }

        public static void Main(string[] args)
        {
            Run(args[0], args.Skip(1).ToList());
        }
    }
}
