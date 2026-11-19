using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks.Dataflow;

namespace Confluent.Kafka.Examples.ConsumerExample;

public class ParallelProcess
{
    /// <summary>
    ///     In this example
    ///         - offsets are automatically committed.
    ///         - no extra thread is created for the Poll (Consume) loop.
    /// </summary>
    public static void Run_Consume(string brokerList, List<string> topics, CancellationToken cancellationToken)
    {
        int blocksCount = Environment.ProcessorCount;
        List<ActionBlock<(ConsumeResult<Ignore, string>, CommitingQueue)>> blocks;
        blocks = new(blocksCount);
        for (int i = 0; i < blocks.Count; i++)
        {
            blocks[i] = new ActionBlock<(ConsumeResult<Ignore, string>, CommitingQueue)>(ProcessMessage);
        }

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
                       var remaining = c.Assignment.Where(atp =>
                           partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
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
            CommitingQueue commitingQueue = new CommitingQueue(consumer);

            try
            {
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cancellationToken);

                        if (consumeResult.IsPartitionEOF)
                        {
                            Console.WriteLine(
                                $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                            continue;
                        }

                        commitingQueue.Enqueue(consumeResult);
                        blocks[consumeResult.Partition.Value % blocksCount].Post((consumeResult, commitingQueue));
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

    public static void ProcessMessage((ConsumeResult<Ignore, string> consumeResult, CommitingQueue queue) input)
    {
        try
        {
            Console.WriteLine(
                $"Received message at {input.consumeResult.TopicPartitionOffset}: {input.consumeResult.Message.Value}");
            input.queue.StoreOffset(input.consumeResult);
        }
        catch (KafkaException e)
        {
            Console.WriteLine($"Store Offset error: {e.Error.Reason}");
        }
    }
}