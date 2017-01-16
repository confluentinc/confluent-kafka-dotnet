using System;


namespace Confluent.Kafka.Benchmark
{
    public class Program
    {
        public static void Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.WriteLine($"Usage: dotnet run <broker,broker..> <topic>");
                return;
            }

            var bootstrapServers = args[0];
            var topic = args[1];

            const int NUMBER_OF_MESSAGES = 5000000;
            const int NUMBER_OF_TESTS = 1;

            BenchmarkProducer.TaskProduce(bootstrapServers, topic, NUMBER_OF_MESSAGES, NUMBER_OF_TESTS);
            var firstMessageOffset = BenchmarkProducer.DeliveryHandlerProduce(bootstrapServers, topic, NUMBER_OF_MESSAGES, NUMBER_OF_TESTS);

            BenchmarkConsumer.Poll(bootstrapServers, topic, firstMessageOffset, NUMBER_OF_MESSAGES, NUMBER_OF_TESTS);
            BenchmarkConsumer.Consume(bootstrapServers, topic, firstMessageOffset, NUMBER_OF_MESSAGES, NUMBER_OF_TESTS);
        }
    }
}
