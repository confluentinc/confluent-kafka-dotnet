using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;


namespace AvroGeneric
{
    class Program
    {
        static void Main(string[] args)
        {
            var s = (RecordSchema)Schema.Parse(
                @"{
                    ""namespace"": ""Confluent.Kafka.Examples.AvroSpecific"",
                    ""type"": ""record"",
                    ""name"": ""User"",
                    ""fields"": [
                        {""name"": ""name"", ""type"": ""string""},
                        {""name"": ""favorite_number"",  ""type"": [""int"", ""null""]},
                        {""name"": ""favorite_color"", ""type"": [""string"", ""null""]}
                    ]
                  }"
            );

            var record = new GenericRecord(s);
            record.Add("name", "my name");
            record.Add("favorite_number", 42);
            record.Add("favorite_color", null);

            var ss = record.Schema.ToString();

            var topic = Guid.NewGuid().ToString();

            var config = new Dictionary<string, object>()
            {
                { "bootstrap.servers", "10.200.7.144:9092" },
                { "schema.registry.url", "10.200.7.144:8081" }
            };
 
            using (var p = new Producer<Null, GenericRecord>(config, null, new AvroSerializer()))
            {
                var dr = p.ProduceAsync(topic, null, record).Result;
            }

            var cconfig = new Dictionary<string, object>()
            {
                { "group.id", Guid.NewGuid().ToString() },
                { "bootstrap.servers", "10.200.7.144:9092" },
                { "schema.registry.url", "10.200.7.144:8081" }
            };

            using (var c = new Consumer<Null, GenericRecord>(cconfig, null, new AvroDeserializer()))
            {
                c.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(topic, 0, 0) });
                c.Consume(out Message<Null, GenericRecord> msg, 20000);
                msg.Value.TryGetValue("name", out object name);
                msg.Value.TryGetValue("favorite_number", out object number);
                msg.Value.TryGetValue("favorite_color", out object color);
            }
        }
    }
}
