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
using Confluent.Kafka.Serialization;


/// <summary>
///     Demonstrates use of the Consumer client.
/// </summary>
namespace Confluent.Kafka.Examples.Consumer
{
    public class Program
    {
        /// <summary>
        //      In this example:
        ///         - offsets are auto commited.
        ///         - consumer.Poll / OnMessage is used to consume messages.
        ///         - no extra thread is created for the Poll loop.
        /// </summary>
        public static void Run_Poll(string brokerList, List<string> topics)
        {
            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", brokerList },
                { "group.id", "csharp-consumer" },
                { "enable.auto.commit", true },  // this is the default
                { "auto.commit.interval.ms", 5000 },
                { "statistics.interval.ms", 60000 },
                { "session.timeout.ms", 6000 },
                { "auto.offset.reset", "smallest" }
            };

            using (var consumer = new Consumer<Ignore, string>(config, null, new StringDeserializer(Encoding.UTF8)))
            {
                // Note: All event handlers are called on the main thread.

                consumer.OnRecord += (_, record)
                    => Console.WriteLine($"Topic: {record.Topic} Partition: {record.Partition} Offset: {record.Offset} {record.Message}");

                consumer.OnPartitionEOF += (_, end)
                    => Console.WriteLine($"Reached end of topic {end.Topic} partition {end.Partition}, next message will be at offset {end.Offset}");

                consumer.OnError += (_, error)
                    => Console.WriteLine($"Error: {error}");

                // Raised on deserialization errors or when a consumed message has an error != NoError.
                consumer.OnConsumeError += (_, record)
                    => Console.WriteLine($"Error consuming from topic/partition/offset {record.Topic}/{record.Partition}/{record.Offset}: {record.Error}");

                consumer.OnOffsetsCommitted += (_, commit) 
                    => Console.WriteLine(
                            commit.Error
                                ? $"Failed to commit offsets: {commit.Error}"
                                : $"Successfully committed offsets: [{string.Join(", ", commit.Offsets)}]");

                // Raised when the consumer is assigned a new set of partitions.
                consumer.OnPartitionsAssigned += (_, partitions) =>
                {
                    Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}], member id: {consumer.MemberId}");
                    // If you don't add a handler to the OnPartitionsAssigned event,
                    // the below .Assign call happens automatically. If you do, you
                    // must call .Assign explicitly in order for the consumer to 
                    // start consuming messages.
                    consumer.Assign(partitions);
                };

                // Raised when the consumer's current assignment set has been revoked.
                consumer.OnPartitionsRevoked += (_, partitions) =>
                {
                    Console.WriteLine($"Revoked partitions: [{string.Join(", ", partitions)}]");
                    // If you don't add a handler to the OnPartitionsRevoked event,
                    // the below .Unassign call happens automatically. If you do, 
                    // you must call .Unassign explicitly in order for the consumer
                    // to stop consuming messages from it's previously assigned 
                    // partitions.
                    consumer.Unassign();
                };

                consumer.OnStatistics += (_, json)
                    => Console.WriteLine($"Statistics: {json}");

                consumer.Subscribe(topics);

                Console.WriteLine($"Subscribed to: [{string.Join(", ", consumer.Subscription)}]");

                var cancelled = false;
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true;  // prevent the process from terminating.
                    cancelled = true;
                };

                Console.WriteLine("Ctrl-C to exit.");
                while (!cancelled)
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
            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", brokerList },
                { "group.id", "csharp-consumer" },
                { "enable.auto.commit", false },
                { "statistics.interval.ms", 60000 },
                { "session.timeout.ms", 6000 },
                { "auto.offset.reset", "smallest" }
            };

            using (var consumer = new Consumer<Ignore, string>(config, null, new StringDeserializer(Encoding.UTF8)))
            {
                // Note: All event handlers are called on the main thread.

                consumer.OnPartitionEOF += (_, end)
                    => Console.WriteLine($"Reached end of topic {end.Topic} partition {end.Partition}, next message will be at offset {end.Offset}");

                consumer.OnError += (_, error)
                    => Console.WriteLine($"Error: {error}");

                // Raised on deserialization errors or when a consumed message has an error != NoError.
                consumer.OnConsumeError += (_, error)
                    => Console.WriteLine($"Consume error: {error}");

                // Raised when the consumer is assigned a new set of partitions.
                consumer.OnPartitionsAssigned += (_, partitions) =>
                {
                    Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}], member id: {consumer.MemberId}");
                    // If you don't add a handler to the OnPartitionsAssigned event,
                    // the below .Assign call happens automatically. If you do, you
                    // must call .Assign explicitly in order for the consumer to 
                    // start consuming messages.
                    consumer.Assign(partitions);
                };

                // Raised when the consumer's current assignment set has been revoked.
                consumer.OnPartitionsRevoked += (_, partitions) =>
                {
                    Console.WriteLine($"Revoked partitions: [{string.Join(", ", partitions)}]");
                    // If you don't add a handler to the OnPartitionsRevoked event,
                    // the below .Unassign call happens automatically. If you do, 
                    // you must call .Unassign explicitly in order for the consumer
                    // to stop consuming messages from it's previously assigned 
                    // partitions.
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
                    ConsumerRecord<Ignore, string> record;
                    if (!consumer.Consume(out record, TimeSpan.FromMilliseconds(100)))
                    {
                        continue;
                    }

                    Console.WriteLine($"Topic: {record.Topic} Partition: {record.Partition} Offset: {record.Offset} {record.Message}");

                    if (record.Offset % 5 == 0)
                    {
                        Console.WriteLine($"Committing offset");
                        var committedOffsets = consumer.Commit(record);
                        Console.WriteLine($"Committed offset: {committedOffsets}");
                    }
                }
            }
        }

        /// <summary>
        ///     In this example
        ///         - consumer group functionality (i.e. .Subscribe + offset commits) is not used.
        ///         - the consumer is manually assigned to a partition and always starts consumption
        ///           from a specific offset (0).
        /// </summary>
        public static void Run_ManualAssign(string brokerList, List<string> topics)
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
                { "enable.auto.commit", false }
            };

            using (var consumer = new Consumer<Ignore, string>(config, null, new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Assign(topics.Select(topic => new TopicPartitionOffset(topic, 0, Offset.Beginning)).ToList());

                // Raised on critical errors, e.g. connection failures or all brokers down.
                consumer.OnError += (_, error)
                    => Console.WriteLine($"Error: {error}");

                // Raised on deserialization errors or when a consumed message has an error != NoError.
                consumer.OnConsumeError += (_, error)
                    => Console.WriteLine($"Consume error: {error}");

                while (true)
                {
                    if (consumer.Consume(out Message<Ignore, string> msg, TimeSpan.FromSeconds(1)))
                    {
                        Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");
                    }
                }
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

            switch (mode)
            {
                case "poll":
                    Run_Poll(brokerList, topics);
                    break;
                case "consume":
                    Run_Consume(brokerList, topics);
                    break;
                case "manual":
                    Run_ManualAssign(brokerList, topics);
                    break;
                default:
                    PrintUsage();
                    break;
            }
        }
    }
}
