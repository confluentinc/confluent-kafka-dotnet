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

using Confluent.Kafka;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Security.Cryptography;
using System.Threading;

namespace Confluent.Kafka.Examples.ProducerExample
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            string brokerList = "localhost:9092";
            string topicName = "test-topic";

            var config = new ProducerConfig { BootstrapServers = brokerList };

            // This CTS lives for the lifetime of the app; before the fix, a synchronous ProduceAsync
            // failure (e.g., broker rejects oversized messages) leaves a cancellation registration
            // rooted in this CTS, which in turn keeps the handler and its payload byte[] alive.
            var cts = new CancellationTokenSource();

            using (var producer = new ProducerBuilder<string, byte[]>(config).Build())
            {
                Console.WriteLine("\n-----------------------------------------------------------------------");
                Console.WriteLine($"Producer {producer.Name} producing on topic {topicName}.");
                Console.WriteLine("-----------------------------------------------------------------------");

                var cancelled = false;
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cancelled = true;
                };

                while (!cancelled)
                {
                    Console.Write("> ");

                    var kafkaHeader = new Headers();

                    // Repro: set broker/topic max.message.bytes below this payload size to trigger
                    // "Broker: Message size too large" and observe retained byte[] instances pre-fix.
                    var binaryMsg = new byte[2_000_000];
                    using (var rng = RandomNumberGenerator.Create())
                    {
                        rng.GetBytes(binaryMsg);
                    }

                    try
                    {
                        var kafkaMsg = new Message<string, byte[]> { Value = binaryMsg, Headers = kafkaHeader };
                        var deliveryReport = await producer.ProduceAsync(topicName, kafkaMsg, cts.Token);

                        Console.WriteLine($"delivered to: {deliveryReport.TopicPartitionOffset}");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"failed to deliver message: {e.Message}");

                        await Task.Delay(200);
                    }

                    // Force GC to make retained byte[] growth visible during repro runs.
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    GC.Collect();
                    
                    var managedBytes = GC.GetTotalMemory(forceFullCollection: false);
                    var managedMb = managedBytes / (1024.0 * 1024.0);
                    Console.WriteLine($"Managed heap: {managedMb:n2} MB");
                }
            }
        }
    }
}
