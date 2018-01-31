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


namespace Confluent.Kafka.Benchmark
{
    public static class BenchmarkProducer
    {
        private class BenchmarkProducerDeliveryHandler : IDeliveryHandler
        {
            public int NumberOfMessages { get; private set; }
            public AutoResetEvent AutoEvent { get; private set; }

            public BenchmarkProducerDeliveryHandler(int numberOfMessages)
            {
                this.NumberOfMessages = numberOfMessages;
                this.AutoEvent = new AutoResetEvent(false);
            }

            public bool MarshalData { get { return false; } }

            public void HandleDeliveryReport(Message deliveryReport)
            {
                if (--NumberOfMessages == 0)
                {
                    AutoEvent.Set();
                }
            }
        }

        private static long BenchmarkProducerImpl(string bootstrapServers, string topic, int nMessages, int nTests, bool useDeliveryHandler)
        {
            // mirrors the librdkafka performance test example.
            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "queue.buffering.max.messages", 2000000 },
                { "message.send.max.retries", 3 },
                { "retry.backoff.ms", 500 },
                { "linger.ms", 100 }
            };

            Message firstDeliveryReport = null;

            using (var producer = new Producer(config))
            {
                for (var j=0; j<nTests; ++j)
                {
                    Console.WriteLine($"{producer.Name} producing on {topic} " + (useDeliveryHandler ? "[DeliveryHandler]" : "[Task]"));

                    byte cnt = 0;
                    var val = new byte[100].Select(a => ++cnt).ToArray();

                    // this avoids including connection setup, topic creation time, etc.. in result.
                    firstDeliveryReport = producer.ProduceAsync(topic, null, val).Result;

                    var startTime = DateTime.Now.Ticks;

                    if (useDeliveryHandler)
                    {
                        var deliveryHandler = new BenchmarkProducerDeliveryHandler(nMessages);

                        for (int i = 0; i < nMessages; i++)
                        {
                            producer.Produce(topic, null, val, deliveryHandler);
                        }

                        deliveryHandler.AutoEvent.WaitOne();
                    }
                    else
                    {
                        var tasks = new Task[nMessages];
                        for (int i = 0; i < nMessages; i++)
                        {
                            tasks[i] = producer.ProduceAsync(topic, null, val);
                        }
                        Task.WaitAll(tasks);
                    }

                    var duration = DateTime.Now.Ticks - startTime;

                    Console.WriteLine($"Produced {nMessages} in {duration/10000.0:F0}ms");
                    Console.WriteLine($"{nMessages / (duration/10000.0):F0} messages/ms");
                }

                producer.Flush(TimeSpan.FromSeconds(10));
            }

            return firstDeliveryReport.Offset;
        }

        /// <summary>
        ///     Producer benchmark masquarading as an integration test.
        ///     Uses Task based produce method.
        /// </summary>
        public static long TaskProduce(string bootstrapServers, string topic, int nMessages, int nTests)
            => BenchmarkProducerImpl(bootstrapServers, topic, nMessages, nTests, false);

        /// <summary>
        ///     Producer benchmark (with custom delivery handler) masquarading
        ///     as an integration test. Uses Task based produce method.
        /// </summary>
        public static long DeliveryHandlerProduce(string bootstrapServers, string topic, int nMessages, int nTests)
            => BenchmarkProducerImpl(bootstrapServers, topic, nMessages, nTests, true);
    }
}
