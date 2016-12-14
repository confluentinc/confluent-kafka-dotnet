using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Confluent.Kafka.SimpleProducer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            string brokerList = args[0];
            var topics = args.Skip(1).ToList();

            var config = new Dictionary<string, object>
            {
                { "group.id", "simple-csharp-consumer" },
                { "bootstrap.servers", brokerList }
            };

            using (var consumer = new Consumer(config))
            {
                consumer.Assign(new List<TopicPartitionOffset> {new TopicPartitionOffset(topics.First(), 0, 5)});

                while (true)
                {
                    var msgMaybe = consumer.Consume();
                    if (msgMaybe != null)
                    {
                        var msg = msgMaybe.Value;
                        string msgText = Encoding.UTF8.GetString(msg.Value, 0, msg.Value.Length);
                        Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msgText}");
                    }
                }
            }
        }
    }
}
