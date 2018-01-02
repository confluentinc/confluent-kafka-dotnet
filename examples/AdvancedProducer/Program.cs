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
using System.IO;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka.Examples.AdvancedProducer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.WriteLine("Usage: .. brokerList topicName");
                return;
            }

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

            var config = new Dictionary<string, object> { { "bootstrap.servers", brokerList } };

            using (var producer = new Producer<string, string>(config, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8)))
            {
                Console.WriteLine("\n-----------------------------------------------------------------------");
                Console.WriteLine($"Producer {producer.Name} producing on topic {topicName}.");
                Console.WriteLine("-----------------------------------------------------------------------");
                Console.WriteLine("To create a kafka message with UTF-8 encoded key/value message:");
                Console.WriteLine("> key value<Enter>");
                Console.WriteLine("To create a kafka message with empty key and UTF-8 encoded value:");
                Console.WriteLine("> value<enter>");
                Console.WriteLine("Ctrl-C to quit.\n");

                var cancelled = false;
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cancelled = true;
                };

                while (!cancelled)
                {
                    Console.Write("> ");

                    string text;
                    try
                    {
                        text = Console.ReadLine();
                    }
                    catch (IOException)
                    {
                        // IO exception is thrown when ConsoleCancelEventArgs.Cancel == true.
                        break;
                    }
                    if (text == null)
                    {
                        // Console returned null before 
                        // the CancelKeyPress was treated
                        break;
                    }

                    var key = "";
                    var val = text;

                    // split line if both key and value specified.
                    int index = text.IndexOf(" ");
                    if (index != -1)
                    {
                        key = text.Substring(0, index);
                        val = text.Substring(index + 1);
                    }

                    var deliveryReport = producer.ProduceAsync(topicName, key, val);
                    var result = deliveryReport.Result; // synchronously waits for message to be produced.
                    Console.WriteLine($"Partition: {result.Partition}, Offset: {result.Offset}");
                }
            }
        }
    }
}
