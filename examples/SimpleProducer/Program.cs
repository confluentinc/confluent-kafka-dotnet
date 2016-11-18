using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka.Serialization;

namespace Confluent.Kafka.SimpleProducer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            string brokerList = args[0];
            string topicName = args[1];

            var config = new Dictionary<string, object> { { "bootstrap.servers", brokerList } };

            using (var producer = new Producer<Null, string>(config))
            {
                // TODO: figure out why the cast below is necessary and how to avoid it.
                // TODO: There should be no need to specify a serializer for common types like string - I think it should default to the UTF8 serializer.
                producer.ValueSerializer = (ISerializer<string>)new Confluent.Kafka.Serialization.Utf8StringSerializer();

                Console.WriteLine($"{producer.Name} producing on {topicName}. q to exit.");

                string text;
                while ((text = Console.ReadLine()) != "q")
                {
                    Task<DeliveryReport> deliveryReport = producer.Produce(topicName, text);
                    var unused = deliveryReport.ContinueWith(task =>
                    {
                        Console.WriteLine($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
                    });
                }
            }
        }
    }
}
