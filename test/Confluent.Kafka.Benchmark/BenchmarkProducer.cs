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
using System.Threading.Tasks;
using System.Threading;
using System.Linq;
using System.Collections.Generic;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka.Benchmark
{
    public static class BenchmarkProducer
    {
        private static long BenchmarkProducerImpl(
            string bootstrapServers, 
            string topic, 
            int nMessages, 
            int nTests, 
            int nHeaders,
            bool useDeliveryHandler)
        {
            // mirrors the librdkafka performance test example.
            var config = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                QueueBufferingMaxMessages = 2000000,
                MessageSendMaxRetries = 3,
                RetryBackoffMs = 500 ,
                LingerMs = 100,
                DeliveryReportFields = "none"
            };

            DeliveryReport<byte[], byte[]> firstDeliveryReport = null;

            Headers headers = null;
            if (nHeaders > 0)
            {
                headers = new Headers();
                for (int i=0; i<nHeaders; ++i)
                {
                    headers.Add($"header-{i+1}", new byte[] { (byte)i, (byte)(i+1), (byte)(i+2), (byte)(i+3) });
                }
            }

            using (var producer = new Producer<byte[], byte[]>(config))
            {
                for (var j=0; j<nTests; j += 1)
                {
                    Console.WriteLine($"{producer.Name} producing on {topic} " + (useDeliveryHandler ? "[Action<Message>]" : "[Task]"));

                    byte cnt = 0;
                    var val = new byte[100].Select(a => ++cnt).ToArray();

                    // this avoids including connection setup, topic creation time, etc.. in result.
                    firstDeliveryReport = producer.ProduceAsync(topic, new Message<byte[], byte[]> { Value = val, Headers = headers }).Result;

                    var startTime = DateTime.Now.Ticks;

                    if (useDeliveryHandler)
                    {
                        var autoEvent = new AutoResetEvent(false);
                        var msgCount = nMessages;
                        Action<DeliveryReportResult<byte[], byte[]>> deliveryHandler = (DeliveryReportResult<byte[], byte[]> deliveryReport) => 
                        {
                            if (--msgCount == 0)
                            {
                                autoEvent.Set();
                            }
                        };

                        for (int i = 0; i < nMessages; i += 1)
                        {
                            producer.BeginProduce(topic, new Message<byte[], byte[]> { Value = val, Headers = headers }, deliveryHandler);
                        }

                        autoEvent.WaitOne();
                    }
                    else
                    {
                        var tasks = new Task[nMessages];
                        for (int i = 0; i < nMessages; i += 1)
                        {
                            tasks[i] = producer.ProduceAsync(topic, new Message<byte[], byte[]> { Value = val, Headers = headers });
                        }
                        Task.WaitAll(tasks);
                    }

                    var duration = DateTime.Now.Ticks - startTime;

                    Console.WriteLine($"Produced {nMessages} messages in {duration/10000.0:F0}ms");
                    Console.WriteLine($"{nMessages / (duration/10000.0):F0}k msg/s");
                }

                producer.Flush(TimeSpan.FromSeconds(10));
            }

            return firstDeliveryReport.Offset;
        }

        /// <summary>
        ///     Producer benchmark masquarading as an integration test.
        ///     Uses Task based produce method.
        /// </summary>
        public static long TaskProduce(string bootstrapServers, string topic, int nMessages, int nHeaders, int nTests)
            => BenchmarkProducerImpl(bootstrapServers, topic, nMessages, nTests, nHeaders, false);

        /// <summary>
        ///     Producer benchmark (with custom delivery handler) masquarading
        ///     as an integration test. Uses Task based produce method.
        /// </summary>
        public static long DeliveryHandlerProduce(string bootstrapServers, string topic, int nMessages, int nHeaders, int nTests)
            => BenchmarkProducerImpl(bootstrapServers, topic, nMessages, nTests, nHeaders, true);
    }
}
