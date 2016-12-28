using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka.Serialization;


/// <summary>
///     Demonstrates use of the deserializing Consumer.
/// </summary>
namespace Confluent.Kafka.AdvancedConsumer
{
    public class Program
    {
        private static Dictionary<string, object> constructConfig(string brokerList, bool enableAutoCommit) =>
            new Dictionary<string, object>
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

        /// <summary>
        //      In this example:
        ///         - offsets are auto commited.
        ///         - OnMessage is used to consume messages.
        ///         - the poll loop is performed on a background thread started using Consumer.Start().
        /// </summary>
        public static void Run_Background(string brokerList, List<string> topics)
        {
            using (var consumer = new Consumer<Null, string>(constructConfig(brokerList, true), null, new StringDeserializer(Encoding.UTF8)))
            {
                // Note: All event handlers are executed on the same thread (the one created/started by the Consumer.Start())

                consumer.OnMessage += (_, msg)
                    => Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");

                consumer.OnPartitionEOF += (_, end)
                    => Console.WriteLine($"Reached end of topic {end.Topic} partition {end.Partition}, next message will be at offset {end.Offset}");

                consumer.OnError += (_, error)
                    => Console.WriteLine($"Error: {error.ErrorCode} {error.Reason}");

                consumer.OnOffsetCommit += (_, commit) =>
                {
                    Console.WriteLine($"[{string.Join(", ", commit.Offsets)}]");

                    if (commit.Error != ErrorCode.NO_ERROR)
                    {
                        Console.WriteLine($"Failed to commit offsets: {commit.Error}");
                    }
                    Console.WriteLine($"Successfully committed offsets: [{string.Join(", ", commit.Offsets)}]");
                };

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

                consumer.OnStatistics += (_, json)
                    => Console.WriteLine($"Statistics: {json}");

                consumer.Subscribe(topics);

                Console.WriteLine($"Subscribed to: [{string.Join(", ", consumer.Subscription)}]");

                consumer.Start();

                Console.WriteLine($"Started consumer, press enter to stop consuming");
                Console.ReadLine();

                consumer.Stop();
            }
        }

        /// <summary>
        //      In this example:
        ///         - offsets are auto commited.
        ///         - consumer.Poll / OnMessage is used to consume messages.
        ///         - the poll loop is performed on a separate thread.
        /// </summary>
        public static void Run_Poll(string brokerList, List<string> topics)
        {
            using (var consumer = new Consumer<Null, string>(constructConfig(brokerList, true), null, new StringDeserializer(Encoding.UTF8)))
            {
                // Note: All event handlers are called on the main thread.

                consumer.OnMessage += (_, msg)
                    => Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");

                consumer.OnPartitionEOF += (_, end)
                    => Console.WriteLine($"Reached end of topic {end.Topic} partition {end.Partition}, next message will be at offset {end.Offset}");

                consumer.OnError += (_, error)
                    => Console.WriteLine($"Error: {error.ErrorCode} {error.Reason}");

                consumer.OnOffsetCommit += (_, commit) =>
                {
                    Console.WriteLine($"[{string.Join(", ", commit.Offsets)}]");

                    if (commit.Error != ErrorCode.NO_ERROR)
                    {
                        Console.WriteLine($"Failed to commit offsets: {commit.Error}");
                    }
                    Console.WriteLine($"Successfully committed offsets: [{string.Join(", ", commit.Offsets)}]");
                };

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

                consumer.OnStatistics += (_, json)
                    => Console.WriteLine($"Statistics: {json}");

                consumer.Subscribe(topics);

                Console.WriteLine($"Subscribed to: [{string.Join(", ", consumer.Subscription)}]");

                var cancelled = false;
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cancelled = true;
                };

                Console.WriteLine("Ctrl-C to exit.");
                while(!cancelled)
                {
                    consumer.Poll(TimeSpan.FromMilliseconds(100));
                }
            }
        }

        /// <summary>
        ///     In this example
        ///         - offsets are manually committed.
        ///         - consumer.Consume is used to consume messages.
        ///             (all other events are still handled by event handlers)
        ///         - no extra thread is created for the Poll (Consume) loop.
        /// </summary>
        public static void Run_Consume(string brokerList, List<string> topics)
        {
            using (var consumer = new Consumer<Null, string>(constructConfig(brokerList, false), null, new StringDeserializer(Encoding.UTF8)))
            {
                // Note: All event handlers are called on the main thread.

                consumer.OnPartitionEOF += (_, end)
                    => Console.WriteLine($"Reached end of topic {end.Topic} partition {end.Partition}, next message will be at offset {end.Offset}");

                consumer.OnError += (_, error)
                    => Console.WriteLine($"Error: {error.ErrorCode} {error.Reason}");

                consumer.OnOffsetCommit += (_, commit) =>
                {
                    Console.WriteLine($"[{string.Join(", ", commit.Offsets)}]");

                    if (commit.Error != ErrorCode.NO_ERROR)
                    {
                        Console.WriteLine($"Failed to commit offsets: {commit.Error}");
                    }
                    Console.WriteLine($"Successfully committed offsets: [{string.Join(", ", commit.Offsets)}]");
                };

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

                consumer.OnStatistics += (_, json)
                    => Console.WriteLine($"Statistics: {json}");

                consumer.Subscribe(topics);

                Console.WriteLine($"Started consumer, Ctrl-C to stop consuming");

                var cancelled = false;
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cancelled = true;
                };

                while (!cancelled)
                {
                    MessageInfo<Null, string> msg;
                    if (!consumer.Consume(out msg, TimeSpan.FromMilliseconds(100)))
                    {
                        continue;
                    }

                    Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");

                    if (msg.Offset % 5 == 0)
                    {
                        Console.WriteLine($"Committing offset");
                        consumer.Commit(msg);
                        Console.WriteLine($"Committed offset");
                    }
                }
            }
        }

        public static void Main(string[] args)
        {
            var mode = args[1];
            var topics = args.Skip(2).ToList();

            switch (args[0])
            {
                case "poll":
                    Run_Poll(mode, topics);
                    break;
                case "consume":
                    Run_Consume(mode, topics);
                    break;
                case "background":
                    Run_Background(mode, topics);
                    break;
            }
        }
    }
}
