using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Confluent.Kafka.Transactions
{
    /// <summary>
    ///     Uses the specified number of producers to concurrently produce runs of messages
    ///     in committed or aborted transactions (randomly) and then consume them, checking
    ///     that the content / ordering of the messages is as expected.
    ///
    ///     message key:     producer id.
    ///     message value:   an incrementing id for messages in committed transactions, -1 otherwise.
    ///
    ///     stdout:
    ///       +n  - a message that will be in a committed transaction was produced with this seq number.
    ///       .   - a message was consumed, and the seq number was as expected.
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 3)
            {
                Console.WriteLine($"Usage: .. <broker,broker..> <num_producers> <abort_probability (0..1)>");
                return;
            }
            var bootstrapServers = args[0];
            var numProducers = int.Parse(args[1]);
            var abortProbability = float.Parse(args[2]);

            var conf = new SimulationConfig(
                topic: Guid.NewGuid().ToString(),
                messageCount: 10000,
                randomSeedBase: 1,
                maxRunLength: 10,  // maximum number of messages in any aborted or committed transaction.
                maxPause: 0.05, // maximum time to pause (seconds) between producing each message (per producer).
                probability_abort: abortProbability);

            var tasks = new List<Task>();

            for (int i=0; i<numProducers; ++i)
            {
                var p = new TestProducer(bootstrapServers, i, conf);
                tasks.Add(Task.Run(() => p.Run()));
            }

            var c = new TestConsumer(bootstrapServers, conf);
            tasks.Add(Task.Run(() => c.Run()));

            Task.WaitAll(tasks.ToArray());
        }
    }
}
