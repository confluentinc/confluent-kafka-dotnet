using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka.Serialization;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
///     Demonstrates use of the deserializing Consumer.
/// </summary>
namespace Confluent.Kafka.AdvancedConsumer
{
    public class Program
    {
        /// <summary>
        //      In this example:
        ///         - offsets are auto commited.
        ///         - consumer.Poll / OnMessage is used to consume messages.
        ///         - the poll loop is performed on a separate thread.
        /// </summary>
        public static void Run_Poll(string brokerList, List<string> topics)
        {
            var config = new Dictionary<string, object>
            {
                { "group.id", "advanced-csharp-consumer" },
                { "enable.auto.commit", true },
                { "auto.commit.interval.ms", 5000 },
                { "statistics.interval.ms", 60000 },
                { "bootstrap.servers", brokerList },
                { "default.topic.config", new Dictionary<string, object>()
                    {
                        { "auto.offset.reset", "smallest" }
                    }
                }
            };

            using (var consumer = new Consumer<Null, string>(config, null, new StringDeserializer(Encoding.UTF8)))
            {
                // Note: All events are called on the same thread (the consumer.Poll thread started below).

                consumer.OnMessage += (_, msg) =>
                {
                    Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");
                };

                consumer.OnPartitionEOF += (_, end) =>
                {
                    Console.WriteLine($"Reached end of topic {end.Topic} partition {end.Partition}, next message will be at offset {end.Offset}");
                };

                consumer.OnError += (_, error) =>
                {
                    Console.WriteLine($"Error: {error.ErrorCode} {error.Reason}");
                };

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

                consumer.OnStatistics += (_, json) =>
                {
                    Console.WriteLine($"Statistics: {json}");
                };

                consumer.Subscribe(topics);

                Task consumerTask;
                var consumerCts = new CancellationTokenSource();
                var ct = consumerCts.Token;
                consumerTask = Task.Factory.StartNew(() =>
                {
                    while (!ct.IsCancellationRequested)
                    {
                        consumer.Poll(TimeSpan.FromMilliseconds(100));
                    }
                }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);

                Console.WriteLine($"Assigned to: [{string.Join(", ", consumer.Assignment)}]");
                Console.WriteLine($"Subscribed to: [{string.Join(", ", consumer.Subscription)}]");

                Console.WriteLine($"Started consumer, press enter to stop consuming");
                Console.ReadLine();

                consumerCts.Cancel();
                consumerTask.Wait();
            }
        }

        /// <summary>
        ///     In this example
        ///         - offsets are manually committed.
        ///         - consumer.Consume is used to consume messages.
        ///             (all other events are still handled by event handlers)
        ///         - no extra thread is created for the Poll (Consume) loop.
        /// </summary>
        /// <remarks>
        ///     TODO: we may remove consumer.Consume method if, after performance
        ///     tuning, the performance hit in using C# events for message
        ///     consumption is deemed insignificant compared to other factors.
        /// </remarks>
        public static void Run_Consume(string brokerList, List<string> topics)
        {
            var config = new Dictionary<string, object>
            {
                { "group.id", "advanced-csharp-consumer" },
                { "enable.auto.commit", false },
                { "auto.commit.interval.ms", 5000 },
                { "statistics.interval.ms", 60000 },
                { "bootstrap.servers", brokerList },
                { "default.topic.config", new Dictionary<string, object>()
                    {
                        { "auto.offset.reset", "smallest" }
                    }
                }
            };

            using (var consumer = new Consumer<Null, string>(config, null, new StringDeserializer(Encoding.UTF8)))
            {
                consumer.OnPartitionEOF += (_, end) =>
                {
                    Console.WriteLine($"Reached end of topic {end.Topic} partition {end.Partition}, next message will be at offset {end.Offset}");
                };

                consumer.OnError += (_, error) =>
                {
                    Console.WriteLine($"Error: {error.ErrorCode} {error.Reason}");
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

                consumer.OnStatistics += (_, json) =>
                {
                    Console.WriteLine($"Statistics: {json}");
                };

                consumer.Subscribe(topics);

                Console.WriteLine($"Started consumer, Ctrl-C to stop consuming");

                var cancelled = false;
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cancelled = true;
                };

                while (!cancelled)
                {
                    // Note: This is more awkward than using consumer.Poll / OnMessage, but it's also more efficient.
                    var msgMaybe = consumer.Consume(TimeSpan.FromMilliseconds(100));
                    if (!msgMaybe.HasValue)
                    {
                        continue;
                    }
                    var msg = msgMaybe.Value;

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
            if (args[0] == "poll")
            {
                Run_Poll(args[1], args.Skip(2).ToList());
            }
            else
            {
                Run_Consume(args[1], args.Skip(2).ToList());
            }
        }
    }
}
