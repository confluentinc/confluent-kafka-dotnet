using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Confluent.Kafka.Transactions
{
    class Program
    {
        static int NumberOfProducers = 3;

        static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine($"Usage: .. <broker,broker..>");
                return;
            }
            var bootstrapServers = args[0];

            var conf = new SimulationConfig(
                topic: Guid.NewGuid().ToString(),
                messageCount: 1000000,
                randomSeedBase: 1, // 0 causes consumer hang.
                maxRunLength: 10,
                maxPause: 0.2,
                probability_abort: 0.4);

            var tasks = new List<Task>();

            for (int i=0; i<NumberOfProducers; ++i)
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
