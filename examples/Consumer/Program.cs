// Copyright 2016-2018 Confluent Inc., 

using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Consumer;

public class Program
{
    /// <summary>
    ///     In this example
    ///     - offsets are automatically committed.
    ///     - no extra thread is created for the Poll (Consume) loop.
    /// </summary>
    public static void Run_Consume(string brokerList, List<string> topics, CancellationToken cancellationToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = brokerList,
            GroupId = "csharp-consumer",
            EnableAutoOffsetStore = false,
            EnableAutoCommit = true,
            StatisticsIntervalMs = 5000,
            SessionTimeoutMs = 6000,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = true,
            SaslUsername = "sophis-sophis-jna",
            SaslPassword = "vq8NKGA84Zadd",
            SaslMechanism = SaslMechanism.Plain,
            SecurityProtocol = SecurityProtocol.SaslSsl,
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
                   .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}")).SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}")).SetPartitionsAssignedHandler(
                       (c, partitions) =>
                       {
                           // Since a cooperative assignor (CooperativeSticky) has been configured, the
                           // partition assignment is incremental (adds partitions to any existing assignment).
                           Console.WriteLine(
                               $"Partitions incrementally assigned: [{string.Join(',', partitions.Select(p => p.Partition.Value))}], all: [{string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value))}]");

                           // Possibly manually specify start offsets by returning a list of topic/partition/offsets
                           // to assign to, e.g.:
                           // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                       }).SetPartitionsRevokedHandler((c, partitions) =>
                   {
                       // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
                       // assignment is incremental (may remove only some partitions of the current assignment).
                       var remaining = c.Assignment.Where(atp => partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
                       Console.WriteLine(
                           $"Partitions incrementally revoked: [{string.Join(',', partitions.Select(p => p.Partition.Value))}], remaining: [{string.Join(',', remaining.Select(p => p.Partition.Value))}]");
                   }).SetPartitionsLostHandler((c, partitions) =>
                   {
                       // The lost partitions handler is called when the consumer detects that it has lost ownership
                       // of its assignment (fallen out of the group).
                       Console.WriteLine($"Partitions were lost: [{string.Join(", ", partitions)}]");
                   }).Build())
        {
            consumer.Subscribe(topics);

            try
            {
                while (true)
                    try
                    {
                        var consumeResult = consumer.Consume(cancellationToken);

                        if (consumeResult.IsPartitionEOF)
                        {
                            Console.WriteLine($"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                            continue;
                        }

                        Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");
                        try
                        {
                            // Store the offset associated with consumeResult to a local cache. Stored offsets are committed to Kafka by a background thread every AutoCommitIntervalMs. 
                            // The offset stored is actually the offset of the consumeResult + 1 since by convention, committed offsets specify the next message to consume. 
                            // If EnableAutoOffsetStore had been set to the default value true, the .NET client would automatically store offsets immediately prior to delivering messages to the application. 
                            // Explicitly storing offsets after processing gives at-least once semantics, the default behavior does not.
                            consumer.StoreOffset(consumeResult);
                        }
                        catch (KafkaException e)
                        {
                            Console.WriteLine($"Store Offset error: {e.Error.Reason}");
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Consume error: {e.Error.Reason}");
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
    ///     In this example
    ///     - consumer group functionality (i.e. .Subscribe + offset commits) is not used.
    ///     - the consumer is manually assigned to a partition and always starts consumption
    ///     from a specific offset (0).
    /// </summary>
    public static void Run_ManualAssign(string brokerList, List<string> topics, CancellationToken cancellationToken)
    {
        var config = new ConsumerConfig
        {
            // the group.id property must be specified when creating a consumer, even 
            // if you do not intend to use any consumer group functionality.
            GroupId = "groupid-not-used-but-mandatory",
            BootstrapServers = brokerList,
            // partition offsets can be committed to a group even by consumers not
            // subscribed to the group. in this example, auto commit is disabled
            // to prevent this from occurring.
            EnableAutoCommit = false
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}")).Build())
        {
            consumer.Assign(topics.Select(topic => new TopicPartitionOffset(topic, 0, Offset.Beginning)).ToList());

            try
            {
                while (true)
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
            catch (OperationCanceledException)
            {
                Console.WriteLine("Closing consumer.");
                consumer.Close();
            }
        }
    }

    private static void PrintUsage()
    {
        Console.WriteLine("Usage: .. <subscribe|manual> <broker,broker,..> <topic> [topic..]");
    }

    public static void Main(string[] args)
    {
        args = new string[3];

        args[0] = "subscribe";
        args[1] =
            "lynqs-kafka-broker-01.lynqs.dev.gcp.fpprod.corp:9092,lynqs-kafka-broker-02.lynqs.dev.gcp.fpprod.corp:9092,lynqs-kafka-broker-03.lynqs.dev.gcp.fpprod.corp:9092,lynqs-kafka-broker-04.lynqs.dev.gcp.fpprod.corp:9092,lynqs-kafka-broker-05.lynqs.dev.gcp.fpprod.corp:9092,lynqs-kafka-broker-06.lynqs.dev.gcp.fpprod.corp:9092";

        args[2] = "DEV_HZNSAT_PUBLIC_BxEodTrades_1";


        if (args.Length < 3)
        {
            PrintUsage();
            return;
        }

        var mode = args[0];
        var brokerList = args[1];
        var topics = args.Skip(2).ToList();

        Console.WriteLine("Started consumer, Ctrl-C to stop consuming");

        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
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