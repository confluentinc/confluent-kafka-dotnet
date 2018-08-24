// Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider
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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Serialization;


/// <summary>
///     Demonstrates use of the Consumer client.
/// </summary>
namespace Confluent.Kafka.Examples.Consumer
{
    public class Program
    {
        /// <summary>
        ///     In this example
        ///         - offsets are manually committed.
        ///         - no extra thread is created for the Poll (Consume) loop.
        /// </summary>
        public static void Run_Consume(string brokerList, List<string> topics, CancellationToken cancellationToken)
        {
            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", brokerList },
                { "group.id", "csharp-consumer" },
                { "enable.auto.commit", false },
                { "statistics.interval.ms", 60000 },
                { "session.timeout.ms", 6000 },
                { "auto.offset.reset", "smallest" },
                // note: typically, you should treat error_cb events as information only.
                { "error_cb", (Action<Error>)(error => Console.WriteLine("Error: " + error.Reason)) },
                { "stats_cb", (Action<string>)(json => Console.WriteLine($"Statistics: {json}")) }
            };

            using (var consumer = new Consumer<Ignore, string>(config, null, new StringDeserializer(Encoding.UTF8)))
            {
                // Note: All event handlers are called on the main .Consume thread.
                
                // Raised when the consumer has been notified of a new assignment set.
                consumer.OnPartitionsAssigned += (_, partitions)
                    => Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}], member id: {consumer.MemberId}");

                // Raised when the consumer's current assignment set has been revoked.
                consumer.OnPartitionsRevoked += (_, partitions)
                    => Console.WriteLine($"Revoked partitions: [{string.Join(", ", partitions)}]");

                consumer.OnPartitionEOF += (_, tpo)
                    => Console.WriteLine($"Reached end of topic {tpo.Topic} partition {tpo.Partition}, next message will be at offset {tpo.Offset}");

                consumer.Subscribe(topics);

                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cancellationToken);
                        Console.WriteLine($"Topic: {consumeResult.Topic} Partition: {consumeResult.Partition} Offset: {consumeResult.Offset} {consumeResult.Value}");

                        if (consumeResult.Offset % 5 == 0)
                        {
                            var committedOffsets = consumer.Commit(consumeResult);
                            Console.WriteLine($"Committed offset: {committedOffsets}");
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Consume error: {e.Error}");
                    }
                }

                consumer.Close();
            }
        }

        /// <summary>
        ///     In this example
        ///         - consumer group functionality (i.e. .Subscribe + offset commits) is not used.
        ///         - the consumer is manually assigned to a partition and always starts consumption
        ///           from a specific offset (0).
        /// </summary>
        public static void Run_ManualAssign(string brokerList, List<string> topics, CancellationToken cancellationToken)
        {
            var config = new Dictionary<string, object>
            {
                // the group.id property must be specified when creating a consumer, even 
                // if you do not intend to use any consumer group functionality.
                { "group.id", new Guid().ToString() },
                { "bootstrap.servers", brokerList },
                // partition offsets can be committed to a group even by consumers not
                // subscribed to the group. in this example, auto commit is disabled
                // to prevent this from occuring.
                { "enable.auto.commit", false },
                // note: typically, you should treat error_cb events as information only.
                { "error_cb", (Action<Error>)(error => Console.WriteLine("Error: " + error.Reason)) }
            };

            using (var consumer = new Consumer<Ignore, string>(config, null, new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Assign(topics.Select(topic => new TopicPartitionOffset(topic, 0, Offset.Beginning)).ToList());

                consumer.OnPartitionEOF += (_, topicPartitionOffset)
                    => Console.WriteLine($"End of partition: {topicPartitionOffset}");

                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cancellationToken);
                        Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: ${consumeResult.Message}");
                        consumer.Commit(consumeResult);
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Consume error: {e.Error}");
                        break;
                    }
                }

                consumer.Close();
            }
        }

        private static void PrintUsage()
            => Console.WriteLine("Usage: .. <poll|consume|manual> <broker,broker,..> <topic> [topic..]");

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
                case "consume":
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
