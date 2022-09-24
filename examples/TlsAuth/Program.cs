// Copyright 2022 Confluent Inc.
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
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;
using Confluent.Kafka;


namespace Confluent.Kafka.Examples.TlsAuth
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            if (args.Length < 4 || (args[2] == "auth" && args.Length < 6))
            {
                Console.WriteLine("Usage: .. brokerList topicName noauth|auth ssl-ca-location [ssl-keystore-location] [ssl-keystore-password]");
                return;
            }

            string bootstrapServers = args[0];
            string topicName = args[1];

            ProducerConfig config = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.Ssl,
                SslCaLocation = args[3]
            };

            if (args[2] == "auth")
            {
                config.SslKeystoreLocation = args[4];
                config.SslKeystorePassword = args[5];
            }

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                Console.Write("Type some text followed by return: ");
                var text = Console.ReadLine();
                var deliveryReport = await producer.ProduceAsync(topicName, new Message<Null, string> { Value = text });
                Console.WriteLine($"Wrote '{deliveryReport.Message.Value}' to partition: {deliveryReport.Partition} at offset: {deliveryReport.Offset}");
            }
        }
    }
}
