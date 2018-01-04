
using System;

namespace Confluent.Kafka.AvroIntegrationTests
{
    public class Program
    {
        private static void PrintUsage()
            => Console.WriteLine("Usage: .. <schema_registry_server> <kafka_broker>");

        public static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                PrintUsage();
                return;
            }

            Tests.ProduceConsume(args[0], args[1]);
        }
    }
}
