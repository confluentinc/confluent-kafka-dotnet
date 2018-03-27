// Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider
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
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka.Examples.SimpleConsumer
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

            using (var consumer = new Consumer<Ignore, string>(config, null, new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(topics.First(), 0, 0) });
                
                // Raised on critical errors, e.g. connection failures or all brokers down.
                consumer.OnError += (_, error)
                    => Console.WriteLine($"Error: {error}");

                // Raised on deserialization errors or when a consumed message has an error != NoError.
                consumer.OnConsumeError += (_, error)
                    => Console.WriteLine($"Consume error: {error}");
                
                while (true)
                {
                    if (consumer.Consume(out ConsumerRecord<Ignore, string> record, TimeSpan.FromSeconds(1)))
                    {
                        Console.WriteLine($"Topic: {record.Topic} Partition: {record.Partition} Offset: {record.Offset} {record.Message}");
                    }
                }
            }
        }
    }
}
