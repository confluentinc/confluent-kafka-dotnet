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
        public enum Mode
        {
            UntypedDeliveryHandler,
            TypedDeliveryHandler,
            TypedTask
        }

        private class BenchmarkProducerDeliveryHandler : IDeliveryHandler
        {
            public int NumberOfMessages { get; private set; }
            public AutoResetEvent AutoEvent { get; private set; }
            public Message LastDeliveryReport { get; private set; }

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
                    LastDeliveryReport = deliveryReport;
                }
            }
        }

        private class BenchmarkTypedProducerDeliveryHandler : IDeliveryHandler<Null, byte[]>
        {
            public int NumberOfMessages { get; private set; }
            public AutoResetEvent AutoEvent { get; private set; }
            public Message<Null, byte[]> LastDeliveryReport { get; private set; }

            public BenchmarkTypedProducerDeliveryHandler(int numberOfMessages)
            {
                this.NumberOfMessages = numberOfMessages;
                this.AutoEvent = new AutoResetEvent(false);
            }
            
            public bool MarshalData { get { return false; } }

            public void HandleDeliveryReport(Message<Null, byte[]> deliveryReport)
            {
                if (--NumberOfMessages == 0)
                {
                    AutoEvent.Set();
                    LastDeliveryReport = deliveryReport;
                }
            }
        }

        private static long BenchmarkProducerImpl(string bootstrapServers, string topic, int nMessages, int nTests, Mode benchmarkMode)
        {
            // mirrors the librdkafka performance test example.
            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "queue.buffering.max.messages", 2000000 },
                { "message.send.max.retries", 3 },
                { "retry.backoff.ms", 500 }
            };

            Offset firstOffset = Offset.Invalid;

            using (var producer = new Producer(config))
            {
                for (var j=0; j<nTests; ++j)
                {
                    Console.WriteLine($"{producer.Name} producing on {topic} [{benchmarkMode.ToString()}]");

                    byte cnt = 0;
                    var val = new byte[100].Select(a => ++cnt).ToArray();

                    // this avoids including connection setup, topic creation time, etc.. in result.

                    var startTime = DateTime.Now.Ticks;

                    if (benchmarkMode == Mode.TypedDeliveryHandler)
                    {
                        var serializingProducer = producer.GetSerializingProducer(new NullSerializer(), new ByteArraySerializer());

                        var firstDeliveryHandler = new BenchmarkTypedProducerDeliveryHandler(1);
                        serializingProducer.ProduceAsync(topic, null, val, firstDeliveryHandler);
                        firstDeliveryHandler.AutoEvent.WaitOne();
                        firstOffset = firstDeliveryHandler.LastDeliveryReport.Offset;

                        var deliveryHandler = new BenchmarkTypedProducerDeliveryHandler(nMessages);
                        for (int i = 0; i < nMessages; i++)
                        {
                            serializingProducer.ProduceAsync(topic, null, val, deliveryHandler);
                        }

                        deliveryHandler.AutoEvent.WaitOne();
                    }
                    else if(benchmarkMode == Mode.TypedTask)
                    {
                        var serializingProducer = producer.GetSerializingProducer<Null, byte[]>(new NullSerializer(), new ByteArraySerializer());
                        firstOffset = serializingProducer.ProduceAsync(topic, null, val).Result.Offset;

                        var tasks = new Task[nMessages];
                        for (int i = 0; i < nMessages; i++)
                        {
                            tasks[i] = serializingProducer.ProduceAsync(topic, null, val);
                        }
                        Task.WaitAll(tasks);
                    }
                    else
                    {
                        var firstDeliveryHandler = new BenchmarkProducerDeliveryHandler(1);
                        producer.Produce(topic, null, 0, 0, val, 0, val.Length, 0, -1, true, firstDeliveryHandler);
                        firstDeliveryHandler.AutoEvent.WaitOne();
                        firstOffset = firstDeliveryHandler.LastDeliveryReport.Offset;

                        var deliveryHandler = new BenchmarkProducerDeliveryHandler(nMessages);

                        for (int i = 0; i < nMessages; i++)
                        {
                            producer.Produce(topic, null, 0, 0, val, 0, val.Length, 0, -1, true, deliveryHandler);
                        }
                        deliveryHandler.AutoEvent.WaitOne();
                    }

                    var duration = DateTime.Now.Ticks - startTime;

                    Console.WriteLine($"Produced {nMessages} in {duration/10000.0:F0}ms");
                    Console.WriteLine($"{nMessages / (duration/10000.0):F0} messages/ms");
                }

                producer.Flush();
            }

            return firstOffset;
        }

        /// <summary>
        ///     Typed Producer benchmark masquarading as an integration test.
        ///     Uses Task based produce method.
        /// </summary>
        public static long TypedTaskProduce(string bootstrapServers, string topic, int nMessages, int nTests)
            => BenchmarkProducerImpl(bootstrapServers, topic, nMessages, nTests, Mode.TypedTask);

        /// <summary>
        ///     Typed Producer benchmark (with custom delivery handler) masquarading
        ///     as an integration test.
        /// </summary>
        public static long TypedDeliveryHandlerProduce(string bootstrapServers, string topic, int nMessages, int nTests)
            => BenchmarkProducerImpl(bootstrapServers, topic, nMessages, nTests, Mode.TypedDeliveryHandler);

        /// <summary>
        ///     Non typed Producer benchmark masquaring as an integration test.
        /// </summary>
        public static long UntypedDeliveryHandlerProduce(string bootstrapServers, string topic, int nMessages, int nTests)
            => BenchmarkProducerImpl(bootstrapServers, topic, nMessages, nTests, Mode.UntypedDeliveryHandler);
    }
}
