// Copyright 2019 Confluent Inc.
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

using System;
using System.Threading.Tasks;
using Confluent.Kafka;


namespace IdempotentProducer
{
    class Program
    {
        async static Task Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.WriteLine("Usage: .. brokerList topicName");
                return;
            }

            var bootstrapServers = args[0];
            var topicName = args[1];

            var config = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                EnableIdempotence = true
            };

            using (var producer = new ProducerBuilder<Null, string>(config)
                .SetErrorHandler((c, e) => 
                {
                    // Fatal errors will be reported first in this error handler, and are best 
                    // handled here. Any use of a producer instance following a fatal error
                    // will result in a KafkaException (with Error.IsFatal set to true).
                    if (e.IsFatal)
                    {
                        Console.WriteLine($"A fatal error occured: {e.Reason}");
                        Environment.Exit(1);
                    }

                    // The client will automatically recover from all other error events - they
                    // should be considered informational in nature.
                    Console.WriteLine($"A non-fatal error occured: {e.Reason}");
                })
                .Build())
            {
                for (var msgCount = 0; ; msgCount += 1)
                {
                    var msg =  $"idempotent producer example message #{msgCount}";

                    try
                    {
                        var dr = await producer.ProduceAsync(topicName, new Message<Null, string> { Value = msg });
                        Console.WriteLine($"produced message to: {dr.TopicPartitionOffset}");
                    }
                    catch (ProduceException<Null, string> e)
                    {
                        Console.WriteLine($"an error occurred producing message: {e.ToString()}");
                    }
                    
                    // Use the Test API to trigger a fabricated fatal error after some time.
                    if (msgCount == 13)
                    {
                        Test.CauseFatalError(producer.Handle, ErrorCode.OutOfOrderSequenceNumber, "test fatal error");
                    }
                }
            }
        }
    }
}
