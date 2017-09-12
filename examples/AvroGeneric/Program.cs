using Avro;
using Avro.Generic;
using Confluent.Kafka.SchemaRegistry;
using Confluent.Kafka.SchemaRegistry.Serialization;
using Moq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Confluent.Kafka.Examples.AvroGeneric
{
    class Program
    {
        static void Main(string[] args)
        {
            string brokerList = "localhost:9092";
            string topicName = "testAvro";

            string userSchema = File.ReadAllText("User.asvc");

            var schemaRegistry = new Mock<ISchemaRegistryClient>();
            schemaRegistry.Setup(x => x.GetRegistrySubject(It.IsAny<string>(), It.IsAny<bool>())).Returns((string topic, bool isKey) => topic + (isKey ? "-key" : "-value"));

            schemaRegistry.Setup(x => x.RegisterAsync(topicName + "-key", It.IsAny<string>())).ReturnsAsync(1);
            schemaRegistry.Setup(x => x.RegisterAsync(topicName + "-value", It.IsAny<string>())).ReturnsAsync(2);

            schemaRegistry.Setup(x => x.GetSchemaAsync(1)).ReturnsAsync("string");
            schemaRegistry.Setup(x => x.GetSchemaAsync(2)).ReturnsAsync(User._SCHEMA.ToString());

            var producerConfig = new Dictionary<string, object> { { "bootstrap.servers", brokerList } };
            var consumerConfig = new Dictionary<string, object> { { "bootstrap.servers", brokerList }, { "group.id", Guid.NewGuid() } };
            var consumerDynamicConfig = new Dictionary<string, object> { { "bootstrap.servers", brokerList }, { "group.id", Guid.NewGuid() } };

            var deserializer = new AvroDeserializer(schemaRegistry.Object);

            using (var consumer = new Consumer<object, object>(consumerConfig, deserializer, deserializer))
            using (var consumerDynamic = new Consumer<dynamic, dynamic>(consumerDynamicConfig, deserializer, deserializer))
            using (var producer = new Producer<object, object>(producerConfig, new AvroSerializer(schemaRegistry.Object, true), new AvroSerializer(schemaRegistry.Object, false)))
            {
                consumer.OnMessage += (o, e) =>
                {
                    var record = (Avro.Generic.GenericRecord)e.Value;
                    Console.WriteLine($"object {e.Key}: read user {record["name"]} with favorite number {record["favorite_number"]}");
                };

                consumerDynamic.OnMessage += (o, e) =>
                {
                    Console.WriteLine($"dynamic {e.Key}: read user {e.Value["name"]} with favorite number {e.Value["favorite_number"]}");
                };

                consumer.Subscribe(topicName);
                consumerDynamic.Subscribe(topicName);
                bool done = false;
                var consumeTask = Task.Factory.StartNew(() =>
                {
                    while (!done)
                    {
                        consumer.Poll(100);
                        consumerDynamic.Poll(100);
                    }
                });

                Console.WriteLine($"{producer.Name} producing on {topicName}. q to exit.");

                int i = 0;
                string text;
                while ((text = Console.ReadLine()) != "q")
                {
                    object user;
                    if (i % 2 == 0)
                    {
                        // serializing using a generated type
                        user = new User { name = text, favorite_color = "green", favorite_number = i++ };
                    }
                    else
                    {
                        // serializing using a generic schema
                        // note that you can reuse the same record and change its value
                        var record = new GenericRecord(Avro.Schema.Parse(userSchema) as RecordSchema);
                        record.Add("name", text);
                        record.Add("favorite_color", "green");
                        record.Add("favorite_number", i++);
                        user = record;
                    }
                    var deliveryReport = producer.ProduceAsync(topicName, text, user);
                    deliveryReport.ContinueWith(task =>
                    {
                        Console.WriteLine($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
                    });
                }
                // Tasks are not waited on synchronously (ContinueWith is not synchronous),
                // so it's possible they may still in progress here.
                producer.Flush(TimeSpan.FromSeconds(10));
                done = true;
                consumeTask.Wait();
            }
        }
    }
}
