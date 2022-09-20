// Copyright 2016-2018 Confluent Inc., 2015-2016 Andreas Heider
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;


/// <summary>
///     Demonstrates use of the Consumer client.
/// </summary>
namespace Confluent.Kafka.Examples.ConsumerExample
{
    public class Program
    {
        /// <summary>
        ///     In this example:
        ///         - The consumer operates in a group - you can start multiple instances
        ///           of the application, and they will share consumption from the topics.
        ///         - Offsets are committed using the StoreOffset mechanism (best practice
        ///           for at-least once semantics).
        /// </summary>
        public static void Run_Consume(string brokerList, List<string> topics, CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                GroupId = "csharp-consumer-group",
                // The default value for both EnableAutoCommit and EnableAutoOffsetStore is true.
                // When EnableAutoOffsetStore is true, offsets are marked for auto-commit immediately
                // prior to being returned to the application from the Consume method. Keeping in
                // mind that offsets are committed periodically (at interval AutoCommitIntervalMs),
                // in a background thread, this gives neither at-least once or at-most once semantics:
                //  - If the application crashes before processing of the message is complete, but
                //    after the background thread has sent the offset commit protocol request to the
                //    group coordinator, then the message will not be processed when consumption starts
                //    again for this partition.
                //  - If the application crashes after processing of the message is complete, but
                //    before the background thread has sent the offset commit protocol request to
                //    the group coordinator, then the message will be processed a second time when
                //    consumption starts again for this partition.
                // Note: With the Java client, default auto commit capability *does* give at-least once
                // semantics because the protocol request is sent as a side effect of the call to 
                // poll.
                // With librdkafka based clients, the best way to achieve at-least once semantics
                // is to set EnableAutoOffsetStore to false, and use the StoreOffset method to mark
                // an offset for commit after processing is complete.
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true,
                // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
                // https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
            };

            // Note: If a key or value deserializer is not set (as is the case below), the 
            // deserializer corresponding to the appropriate type from Confluent.Kafka.Deserializers
            // will be used automatically (where available). The default deserializer for string
            // is UTF8. The default deserializer for Ignore returns null for all input data
            // (including non-null data).
            using (var consumer = new ConsumerBuilder<Ignore, string>(config)
                // Note: All handlers are called on the main .Consume thread.
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the
                    // partition assignment is incremental (adds partitions to any existing assignment).
                    Console.WriteLine(
                        "Partitions incrementally assigned: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], all: [" +
                        string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                        "]");

                    // Possibly manually specify start offsets by returning a list of topic/partition/offsets
                    // to assign to, e.g.:
                    // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
                    // assignment is incremental (may remove only some partitions of the current assignment).
                    var remaining = c.Assignment.Where(atp => partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
                    Console.WriteLine(
                        "Partitions incrementally revoked: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], remaining: [" +
                        string.Join(',', remaining.Select(p => p.Partition.Value)) +
                        "]");
                })
                .SetPartitionsLostHandler((c, partitions) =>
                {
                    // The lost partitions handler is called when the consumer detects that it has lost ownership
                    // of its assignment (fallen out of the group).
                    Console.WriteLine($"Partitions were lost: [{string.Join(", ", partitions)}]");
                })
                .Build())
            {
                consumer.Subscribe(topics);

                try
                {
                    while (true)
                    {
                        try
                        {
                            // In addition to delivering consumed messages (and partition EOF events) to the application,
                            // all handlers (with the exception of the log handler) are called as a side effect of a call
                            // to Consume, on the consume thread.
                            // It is important to note that in the event the group needs to rebalance, the rebalance will
                            // be blocked until the next call is made to Consume if a PartitionsAssigned/Revoked or Lost
                            // handler is configured. If one of these handlers is not configured, default assignment logic
                            // will occur automatically on a background thread and the rebalance will not be blocked.
                            // In most cases, you probably *do* want to delay the rebalance until the next call to Consume,
                            // because this will ensure the consumer still owns the partition associated with the message
                            // just consumed, allowing offsets to be committed or stored successfully.
                            var consumeResult = consumer.Consume(cancellationToken);

                            if (consumeResult.IsPartitionEOF)
                            {
                                Console.WriteLine(
                                    $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                                continue;
                            }

                            Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");

                            // Mark the offset associated with the consumed message as eligible for commit.
                            // We know the consumer still owns the partition because any group rebalance is delayed until
                            // the next call to consume, since we have rebalance handlers configured, and these
                            // execute as a side effect of the next call to Consume.
                            consumer.StoreOffset(consumeResult);
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                    consumer.Close();
                }
            }
        }

        /// <summary>
        ///     In this example:
        ///         - Consumer group functionality (i.e. .Subscribe, offset commit) is not used.
        ///         - The consumer is manually assigned to a partition and always starts consumption
        ///           from the first message in the log.
        /// </summary>
        public static void Run_ManualAssign(string brokerList, List<string> topics, CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                // The GroupId property must be specified when creating a consumer, even 
                // if you do not intend to use any consumer group functionality.
                GroupId = "csharp-example-consumer-group",
                BootstrapServers = brokerList,
                // Partition offsets can be committed to a group even by consumers not
                // subscribed to the group. By default, auto offset commit is enabled.
                // Since we don't make use of committed offsets, disable this.
                EnableAutoCommit = false,
            };

            using (var consumer =
                new ConsumerBuilder<Ignore, string>(config)
                    .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                    .Build())
            {
                consumer.Assign(topics.Select(topic => new TopicPartitionOffset(topic, 0, Offset.Beginning)).ToList());

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cancellationToken);
                            // Note: End of partition notification has not been enabled, so
                            // it is guaranteed that the ConsumeResult instance corresponds
                            // to a Message, and not a PartitionEOF event.
                            Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: ${consumeResult.Message.Value}");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                    consumer.Close();
                }
            }
        }

        private static void PrintUsage()
            => Console.WriteLine("Usage: .. <subscribe|manual> <broker,broker,..> <topic> [topic..]");

        public static void Main(string[] args)
        {
            if (args.Length < 3)
            {
                PrintUsage();
                return;
            }

            var mode = args[0];
            var brokerList = args[1];
            var topics = args.Skip(2).ToList();

            Console.WriteLine($"Started consumer, Ctrl-C to stop consuming");

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            switch (mode)
            {
                case "subscribe":
                    Run_Consume(brokerList, topics, cts.Token);
                    break;
                case "manual":
                    Run_ManualAssign(brokerList, topics, cts.Token);
                    break;
                default:
                    PrintUsage();
                    break;
            }
        }
    }
}
