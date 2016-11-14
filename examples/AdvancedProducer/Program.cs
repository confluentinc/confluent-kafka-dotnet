using System;
using System.Text;
using System.Collections.Generic;
using System.Threading.Tasks;


namespace Confluent.Kafka.AdvancedProducer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            string brokerList = args[0];
            string topicName = args[1];

            /*
            // TODO(mhowlett): allow partitioner to be set.
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
            */

            var config = new Dictionary<string, string> { { "bootstrap.servers", brokerList } };

            using (var producer = new Producer<string, string>(config, null))
            {
                // TODO: work out why explicit cast is needed here.
                // TODO: remove need to explicitly specify string serializers - assume Utf8StringSerializer in Producer as default.
                producer.KeySerializer = (ISerializer<string>)new Confluent.Kafka.Serialization.Utf8StringSerializer();
                producer.ValueSerializer = producer.KeySerializer;

                Console.WriteLine($"{producer.Name} producing on {topicName}. q to exit.");

                string text;
                while ((text = Console.ReadLine()) != "q")
                {
                    string key = "";
                    // Use the first word as the key
                    int index = text.IndexOf(" ");
                    if (index != -1)
                    {
                        key = text.Substring(0, index);
                    }

                    Task<DeliveryReport> deliveryReport = producer.Produce(topicName, key, text);
                    var unused = deliveryReport.ContinueWith(task =>
                    {
                        Console.WriteLine($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
                    });
                }
            }
        }
    }
}
