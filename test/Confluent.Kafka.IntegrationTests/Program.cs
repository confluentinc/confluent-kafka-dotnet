using System;
using System.Linq;
using System.Reflection;


namespace Confluent.Kafka.IntegrationTests
{
    public class Program
    {
        public static void Main(string[] args)
        {
            string bootstrapServers = null;
            string topic = null;
            string testName = null;

            if (args.Length > 0)
            {
                bootstrapServers = args[0];
                if (args.Length > 1)
                {
                    topic = args[1];
                    if (args.Length > 2)
                    {
                        testName = args[2];
                    }
                }
            }
            else
            {
                Console.WriteLine($"Usage: dotnet run <broker> [<topic>]");
                return;
            }

            var methods = System.Reflection.Assembly.GetEntryAssembly().GetTypes()
                    .Select(t => t.GetTypeInfo())
                    .SelectMany(ti => ti.GetMethods())
                    .Where(m => m.GetCustomAttributes(typeof(IntegrationTestAttribute), false).Count() > 0)
                    .ToList();

            foreach (var method in methods)
            {
                if (testName != null)
                {
                    if (method.Name != testName)
                    {
                        continue;
                    }
                }

                if (!((IntegrationTestAttribute)method.GetCustomAttribute(typeof(IntegrationTestAttribute))).ExplicitExecutionRequired || method.Name == testName)
                {
                    Console.WriteLine($"{method.Name}:  starting...");
                    method.Invoke(null, new object[] { bootstrapServers, topic });
                    Console.WriteLine($"{method.Name}:  success!");
                }
            }
        }
    }
}
