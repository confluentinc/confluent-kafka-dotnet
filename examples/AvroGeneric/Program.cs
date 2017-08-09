// Copyright 2016-2017 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using Avro;
using Avro.Generic;
using Confluent.Kafka.SchemaRegistry;
using Confluent.Kafka.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;


namespace Confluent.Kafka.Examples.AvroGeneric
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 3)
            {
                Console.WriteLine("Usage:  AvroGeneric brokerList schemaregistryurl topicName");
                return;
            }
            string brokerList = args[0];
            string schemaRegistryUrl = args[1];
            string topicName = args[2];

            string userSchema = File.ReadAllText("User.asvc");
            
            var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryUrl);

            var producerConfig = new Dictionary<string, object> { { "bootstrap.servers", brokerList } };
            var consumerConfig = new Dictionary<string, object> { { "bootstrap.servers", brokerList }, { "group.id", Guid.NewGuid() } };
            var consumerDynamicConfig = new Dictionary<string, object> { { "bootstrap.servers", brokerList }, { "group.id", Guid.NewGuid() } };

            var deserializer = new AvroDeserializer(schemaRegistry);

            using (var consumer = new Consumer<object, object>(consumerConfig, deserializer, deserializer))
            using (var consumerAvroRecord = new Consumer<string, User>(consumerDynamicConfig, new AvroDeserializer<string>(schemaRegistry), new AvroDeserializer<User>(schemaRegistry)))
            using (var producer = new Producer<string, User>(producerConfig, new AvroSerializer<string>(schemaRegistry, true), new AvroSerializer<User>(schemaRegistry, false)))
            {
                consumer.OnMessage += (o, e) =>
                {
                    // To use when you know the type of data you get (primitive, GenericRecord, Union...)
                    // By using AvroRecord or DataWithId rather than object, you will get schema or schemaId alongside with data
                    var record = (GenericRecord)e.Value;
                    Console.WriteLine($"object {e.Key}: read user {record["name"]} with favorite number {record["favorite_number"]}");
                };
                
                consumerAvroRecord.OnMessage += (o, e) =>
                {
                    //// To use when the type of data read is unknown
                    //// or for custom treatement on given schema
                    //var sb = new StringBuilder();
                    //switch(e.Key.Schema.Tag)
                    //{
                    //    case Avro.Schema.Type.Int:
                    //        sb.Append("Key""Name:" + e.Key.Schema.Name);
                    //        sb.Append(" - ");
                    //        var recordSchema = e.Key.Schema as RecordSchema;
                    //        foreach(var field in recordSchema.Fields)
                    //        {
                    //            sb.Append(field.Name)
                    //            Console.WriteLine()
                    //        }
                    //        break;
                    //}
                    //Console.WriteLine($"dynamic {e.Key}: read user {e.Value["name"]} with favorite number {e.Value["favorite_number"]}");
                };

                consumer.Subscribe(topicName);
                consumerAvroRecord.Subscribe(topicName);
                bool done = false;
                var consumeTask = Task.Factory.StartNew(() =>
                {
                    while (!done)
                    {
                        consumer.Poll(100);
                        consumerAvroRecord.Poll(100);
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
                    var deliveryReport = producer.ProduceAsync(topicName, text, (User)user);
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