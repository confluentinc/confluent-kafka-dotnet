// Copyright 2018 Confluent Inc.
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
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;


namespace ConfluentCloudExample
{
    /// <summary>
    ///     This is a simple example demonstrating how to produce a message to 
    ///     Confluent Cloud then read it back again.
    ///     
    ///     https://www.confluent.io/confluent-cloud/
    /// 
    ///     Confluent Cloud does not auto-create topics. You will need to use the ccloud 
    ///     cli to create the dotnet-test-topic topic before running this example. The 
    ///     <ccloud bootstrap servers>, <ccloud key> and <ccloud secret> parameters are
    ///     available via the confluent cloud web interface. For more information,
    ///     refer to the quick-start:
    ///     
    ///     https://docs.confluent.io/current/cloud-quickstart.html
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            var pConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", "<ccloud bootstrap servers>" },
                { "broker.version.fallback", "0.10.0.0" },
                { "api.version.fallback.ms", 0 },
                { "sasl.mechanisms", "PLAIN" },
                { "security.protocol", "SASL_SSL" },
                // On Windows, default trusted root CA certificates are stored in the Windows Registry. 
                // They are not automatically discovered by Confluent.Kafka and it's not possible to 
                // reference them using the `ssl.ca.location` property. You will need to obtain these 
                // from somewhere else, for example use the cacert.pem file distributed with curl:
                // https://curl.haxx.se/ca/cacert.pem and reference that file in the `ssl.ca.location`
                // property:
                { "ssl.ca.location", "/usr/local/etc/openssl/cert.pem" }, // suitable configuration for linux, osx.
                // { "ssl.ca.location", "c:\\path\\to\\cacert.pem" },     // windows
                { "sasl.username", "<ccloud key>" },
                { "sasl.password", "<ccloud secret>" }
            };

            using (var producer = new Producer<Null, string>(pConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                producer.ProduceAsync("dotnet-test-topic", new Message<Null, string> { Key = null, Value = "test value" })
                    .ContinueWith(result => 
                        {
                            var msg = result.Result;
                            if (msg.Error.Code != ErrorCode.NoError)
                            {
                                Console.WriteLine($"failed to deliver message: {msg.Error.Reason}");
                            }
                            else 
                            {
                                Console.WriteLine($"delivered to: {result.Result.TopicPartitionOffset}");
                            }
                        });

                producer.Flush(TimeSpan.FromSeconds(10));
            }

            var cConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", "<confluent cloud bootstrap servers>" },
                { "broker.version.fallback", "0.10.0.0" },
                { "api.version.fallback.ms", 0 },
                { "sasl.mechanisms", "PLAIN" },
                { "security.protocol", "SASL_SSL" },
                { "ssl.ca.location", "/usr/local/etc/openssl/cert.pem" }, // suitable configuration for linux, osx.
                // { "ssl.ca.location", "c:\\path\\to\\cacert.pem" },     // windows
                { "sasl.username", "<confluent cloud key>" },
                { "sasl.password", "<confluent cloud secret>" },
                { "group.id", Guid.NewGuid().ToString() },
                { "auto.offset.reset", "smallest" }
            };

            using (var consumer = new Consumer<Null, string>(cConfig, null, new StringDeserializer(Encoding.UTF8)))
            { 
                consumer.Subscribe("dotnet-test-topic");

                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(100));
                        if (consumeResult.Message != null)
                        {
                            Console.WriteLine($"consumed: {consumeResult.Value}");
                        }
                        else if (consumeResult.IsPartitionEOF)
                        {
                            Console.WriteLine($"end of partition: {consumeResult.TopicPartitionOffset}");
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"consume error: {e.Error.Reason}");
                    }
                }

                consumer.Close();
            }
        }
    }
}
