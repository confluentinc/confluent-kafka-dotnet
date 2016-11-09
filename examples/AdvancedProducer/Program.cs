using System;
using System.Text;
using System.Threading.Tasks;
using RdKafka;

namespace AdvancedProducer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            string brokerList = args[0];
            string topicName = args[1];

            var topicConfig = new TopicConfig
            {
                CustomPartitioner = (top, key, cnt) =>
                {
                    var kt = (key != null) ? Encoding.UTF8.GetString(key, 0, key.Length) : "(null)";
                    int partition = (key?.Length ?? 0) % cnt;
                    bool available = top.PartitionAvailable(partition);
                    Console.WriteLine($"Partitioner topic: {top.Name} key: {kt} partition count: {cnt} -> {partition} {available}");
                    return partition;
                }
            };

            using (Producer producer = new Producer(brokerList))
            using (Topic topic = producer.Topic(topicName, topicConfig))
            {
                Console.WriteLine($"{producer.Name} producing on {topic.Name}. q to exit.");

                string text;
                while ((text = Console.ReadLine()) != "q")
                {
                    byte[] data = Encoding.UTF8.GetBytes(text);
                    byte[] key = null;
                    // Use the first word as the key
                    int index = text.IndexOf(" ");
                    if (index != -1)
                    {
                        key = Encoding.UTF8.GetBytes(text.Substring(0, index));
                    }

                    Task<DeliveryReport> deliveryReport = topic.Produce(data, key);
                    var unused = deliveryReport.ContinueWith(task =>
                    {
                        Console.WriteLine($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
                    });
                }
            }
        }
    }
}
