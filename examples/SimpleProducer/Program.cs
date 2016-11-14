using System;
using System.Text;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Confluent.Kafka.SimpleProducer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            string brokerList = args[0];
            string topicName = args[1];

            using (var producer = new Producer(new Dictionary<string, string> { { "bootstrap.servers", brokerList } }))
            using (Topic topic = producer.Topic(topicName))
            {
                Console.WriteLine($"{producer.Name} producing on {topic.Name}. q to exit.");

                string text;
                while ((text = Console.ReadLine()) != "q")
                {
                    byte[] data = Encoding.UTF8.GetBytes(text);
                    Task<DeliveryReport> deliveryReport = topic.Produce(data);
                    var unused = deliveryReport.ContinueWith(task =>
                    {
                        Console.WriteLine($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
                    });
                }
            }
        }
    }
}
