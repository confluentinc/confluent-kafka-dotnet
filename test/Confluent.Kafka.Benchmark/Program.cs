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

using System;


namespace Confluent.Kafka.Benchmark
{
    public class Program
    {
        public static void Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.WriteLine($"Usage: .. <broker,broker..> <topic>");
                return;
            }

            var bootstrapServers = args[0];
            var topic = args[1];

            const int NUMBER_OF_MESSAGES = 5000000;
            const int NUMBER_OF_TESTS = 1;

            BenchmarkProducer.TaskProduce(bootstrapServers, topic, NUMBER_OF_MESSAGES, NUMBER_OF_TESTS);
            var firstMessageOffset = BenchmarkProducer.DeliveryHandlerProduce(bootstrapServers, topic, NUMBER_OF_MESSAGES, NUMBER_OF_TESTS);

            BenchmarkConsumer.Poll(bootstrapServers, topic, firstMessageOffset, NUMBER_OF_MESSAGES, NUMBER_OF_TESTS);
            BenchmarkConsumer.Consume(bootstrapServers, topic, firstMessageOffset, NUMBER_OF_MESSAGES, NUMBER_OF_TESTS);
        }
    }
}
