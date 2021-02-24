// Copyright 2020 Confluent Inc.
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
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System.IO;


namespace Confluent.Kafka.Benchmark
{
    public class Latency
    {
        public static void Run(string bootstrapServers, string topicName, string group, int headerCount, int messageSize, int messagesPerSecond, int numberOfMessages, string username, string password)
        {
            if (!Stopwatch.IsHighResolution)
            {
                Console.WriteLine("WARNING: High precision timing is not supported on this platform.");
            }

            if (messagesPerSecond < 1000)
            {
                throw new Exception("Message rate must be >= 1000 msg/s");
            }

            var sw = new Stopwatch();
            sw.Start();

            var monitorObj = new Object();

            var consumerTask = Task.Run(() => {
                // Use middle results only to better estimate steady state performance.
                var trimStart = (long)(numberOfMessages * 0.05);
                var trimEnd = (long)(numberOfMessages * 0.95);
                var results = new long[trimEnd - trimStart];

                var config = new ConsumerConfig
                {
                    GroupId = group,
                    BootstrapServers = bootstrapServers,
                    SessionTimeoutMs = 6000,
                    ConsumeResultFields = headerCount == 0 ? "none" : "headers",
                    QueuedMinMessages = 1000000,
                    AutoOffsetReset = AutoOffsetReset.Latest,
                    EnableAutoCommit = true,
                    SaslUsername = username,
                    SaslPassword = password,
                    SecurityProtocol = username == null ? SecurityProtocol.Plaintext : SecurityProtocol.SaslSsl,
                    SaslMechanism = SaslMechanism.Plain
                };

                using (var consumer = new ConsumerBuilder<Null, byte[]>(config)
                    .SetPartitionsAssignedHandler((c, partitions) => {
                        // Ensure there is no race between consumer determining start offsets and production starting.
                        var initialAssignment = partitions.Select(p => new TopicPartitionOffset(p, c.QueryWatermarkOffsets(p, TimeSpan.FromSeconds(5)).High)).ToList();
                        if (initialAssignment.Where(p => p.Offset != 0).Count() > 0)
                        {
                            Console.WriteLine("Start offsets: [" + String.Join(", ", initialAssignment.OrderBy(a => (int)a.Partition).Select(a => a.Offset.ToString())) + "]");
                        }
                        lock (monitorObj) { Monitor.Pulse(monitorObj); }
                        return initialAssignment;
                    })
                    .Build())
                {
                    consumer.Subscribe(topicName);

                    var count = 0;
                    while (count < numberOfMessages)
                    {
                        var cr = consumer.Consume(1000);
                        if (cr == null)
                        {
                            if (count > 0) { Console.WriteLine($"No message consumed after {count} consumed"); }
                            continue;
                        }
                        if (count < trimStart || count >= trimEnd) { count += 1; continue; }

                        long writeMilliseconds;
                        using (var s = new MemoryStream(cr.Message.Value))
                        using (var br = new BinaryReader(s))
                        {
                            writeMilliseconds = br.ReadInt64();
                        }
                        long elapsedMilliSeconds;
                        lock (sw) { elapsedMilliSeconds = sw.ElapsedMilliseconds; }
                        long latencyMilliSeconds = elapsedMilliSeconds - writeMilliseconds;

                        results[count++ - trimStart] = latencyMilliSeconds;
                        if (count % (numberOfMessages / 10) == 0)
                        {
                            Console.WriteLine($"...{(count / (numberOfMessages/10))}0% complete");
                        }
                    }

                    Console.WriteLine("done");
                    consumer.Close();
                }

                Array.Sort(results);

                Console.WriteLine(
                    "Latency percentiles (ms) [p50: {0}, p75: {1}, p90: {2}, p95: {3}, p99: {4}]",
                    results[(int)(results.Length * 50.0/100.0)],
                    results[(int)(results.Length * 75.0/100.0)],
                    results[(int)(results.Length * 90.0/100.0)],
                    results[(int)(results.Length * 95.0/100.0)],
                    results[(int)(results.Length * 99.0/100.0)]);
            });


            var producerTask = Task.Run(() => {

                lock (monitorObj) { Monitor.Wait(monitorObj); }

                var config = new ProducerConfig
                {
                    BootstrapServers = bootstrapServers,
                    QueueBufferingMaxMessages = 2000000,
                    MessageSendMaxRetries = 3,
                    RetryBackoffMs = 500 ,
                    LingerMs = 5,
                    DeliveryReportFields = "none",
                    EnableIdempotence = true,
                    SaslUsername = username,
                    SaslPassword = password,
                    SecurityProtocol = username == null ? SecurityProtocol.Plaintext : SecurityProtocol.SaslSsl,
                    SaslMechanism = SaslMechanism.Plain
                };

                Headers headers = null;
                if (headerCount > 0)
                {
                    headers = new Headers();
                    for (int i=0; i<headerCount; ++i)
                    {
                        headers.Add($"header-{i+1}", new byte[] { (byte)i, (byte)(i+1), (byte)(i+2), (byte)(i+3) });
                    }
                }
                
                using (var producer = new ProducerBuilder<Null, byte[]>(config).Build())
                {
                    var startMilliseconds = sw.ElapsedMilliseconds;

                    for (int i=0; i<numberOfMessages; ++i)
                    {
                        var payload = new byte[messageSize];

                        long elapsedMilliseconds;
                        lock (sw) { elapsedMilliseconds = sw.ElapsedMilliseconds; }
                        using (var s = new MemoryStream(payload))
                        using (var bw = new BinaryWriter(s))
                        {
                            bw.Write(elapsedMilliseconds);
                        }
                        producer.Produce(topicName, new Message<Null, byte[]> { Value = payload, Headers = headers },
                            dr => { if (dr.Error.Code != ErrorCode.NoError) Console.WriteLine("Message delivery failed: " + dr.Error.Reason); });

                        var desiredProduceCount = (elapsedMilliseconds - startMilliseconds)/1000.0 * messagesPerSecond;

                        // Simple, but about as good as we can do assuming a fast enough rate, and a poor Thread.Sleep precision.
                        if (i > desiredProduceCount)
                        {
                            Thread.Sleep(1);
                        }
                    }

                    while (producer.Flush(TimeSpan.FromSeconds(1)) > 0);

                    long elapsedMilliSeconds;
                    lock (sw) {elapsedMilliSeconds = sw.ElapsedMilliseconds; }
                    Console.WriteLine("Actual throughput: " + (int)Math.Round((numberOfMessages / ((double)(elapsedMilliSeconds - startMilliseconds) / 1000.0))) + " msg/s");
                }
            });

            Task.WaitAll(new [] { producerTask, consumerTask });
        }
    }
}
