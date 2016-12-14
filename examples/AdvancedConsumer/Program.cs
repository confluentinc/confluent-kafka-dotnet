using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;


namespace Confluent.Kafka.AdvancedConsumer
{
    public class Program
    {
        public static void Run(string brokerList, List<string> topics)
        {
            bool enableAutoCommit = true;

            var config = new Dictionary<string, object>
            {
                { "group.id", "advanced-csharp-consumer" },
                { "enable.auto.commit", enableAutoCommit },
                { "auto.commit.interval.ms", 5000 },
                { "statistics.interval.ms", 60000 },
                { "bootstrap.servers", brokerList },
                { "default.topic.config", new Dictionary<string, object>()
                    {
                        { "auto.offset.reset", "smallest" }
                    }
                }
            };

            using (var consumer = new Consumer(config))
            {
                consumer.OnMessage += (_, msg) =>
                {
                    var text = Encoding.UTF8.GetString(msg.Value, 0, msg.Value.Length);
                    Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {text}");

                    if (!enableAutoCommit && msg.Offset % 10 == 0)
                    {
                        Console.WriteLine($"Committing offset");
                        consumer.Commit(msg).Wait();
                        Console.WriteLine($"Committed offset");
                    }
                };

                consumer.PartitionEOF += (_, end) =>
                {
                    Console.WriteLine($"Reached end of topic {end.Topic} partition {end.Partition}, next message will be at offset {end.Offset}");
                };

                consumer.OnError += (_, error) =>
                {
                    Console.WriteLine($"Error: {error.ErrorCode} {error.Reason}");
                };

                if (enableAutoCommit)
                {
                    consumer.OnOffsetCommit += (_, commit) =>
                    {
                        Console.WriteLine($"[{string.Join(", ", commit.Offsets)}]");

                        if (commit.Error != ErrorCode.NO_ERROR)
                        {
                            Console.WriteLine($"Failed to commit offsets: {commit.Error}");
                        }
                        Console.WriteLine($"Successfully committed offsets: [{string.Join(", ", commit.Offsets)}]");
                    };
                }

                consumer.OnPartitionsAssigned += (_, partitions) =>
                {
                    Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}], member id: {consumer.MemberId}");
                    consumer.Assign(partitions.Select(p => new TopicPartitionOffset(p, Offset.Invalid)));
                };

                consumer.OnPartitionsRevoked += (_, partitions) =>
                {
                    Console.WriteLine($"Revoked partitions: [{string.Join(", ", partitions)}]");
                    consumer.Unassign();
                };

                consumer.OnStatistics += (_, json) =>
                {
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
