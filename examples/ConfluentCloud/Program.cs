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
            var pConfig = new ProducerConfig
            {
                BootstrapServers = "<ccloud bootstrap servers>",
                BrokerVersionFallback = "0.10.0.0",
                ApiVersionFallbackMs = 0,
                SaslMechanism = SaslMechanismType.Plain,
                SecurityProtocol = SecurityProtocolType.Sasl_Ssl,
                // On Windows, default trusted root CA certificates are stored in the Windows Registry.
                // They are not automatically discovered by Confluent.Kafka and it's not possible to
                // reference them using the `ssl.ca.location` property. You will need to obtain these
                // from somewhere else, for example use the cacert.pem file distributed with curl:
                // https://curl.haxx.se/ca/cacert.pem and reference that file in the `ssl.ca.location`
                // property:
                SslCaLocation = "/usr/local/etc/openssl/cert.pem", // suitable configuration for linux, osx.
                // SslCaLocation = "c:\\path\\to\\cacert.pem", // windows
                SaslUsername = "<ccloud key>",
                SaslPassword = "<ccloud secret>"
            };

            using (var producer = new Producer<Null, string>(pConfig))
            {
                producer.ProduceAsync("dotnet-test-topic", new Message<Null, string> { Key = null, Value = "test value" })
                    .ContinueWith(task => task.IsFaulted
                        ? $"error producing message: {task.Exception.Message}"
                        : $"produced to: {task.Result.TopicPartitionOffset}");
                
                // block until all in-flight produce requests have completed (successfully
                // or otherwise) or 10s has elapsed.
                producer.Flush(TimeSpan.FromSeconds(10));
            }

            var cConfig = new ConsumerConfig
            {
                BootstrapServers = "<confluent cloud bootstrap servers>",
                BrokerVersionFallback = "0.10.0.0",
                ApiVersionFallbackMs = 0,
                SaslMechanism = SaslMechanismType.Plain,
                SecurityProtocol = SecurityProtocolType.Sasl_Ssl,
                SslCaLocation = "/usr/local/etc/openssl/cert.pem", // suitable configuration for linux, osx.
                // SslCaLocation = "c:\\path\\to\\cacert.pem",     // windows
                SaslUsername = "<confluent cloud key>",
                SaslPassword = "<confluent cloud secret>",
                GroupId = Guid.NewGuid().ToString(),
                AutoOffsetReset = AutoOffsetResetType.Earliest
            };

            using (var consumer = new Consumer<Null, string>(cConfig))
            { 
                consumer.Subscribe("dotnet-test-topic");

                try
                {
                    var consumeResult = consumer.Consume();
                    Console.WriteLine($"consumed: {consumeResult.Value}");
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"consume error: {e.Error.Reason}");
                }

                consumer.Close();
            }

        }
    }
}
