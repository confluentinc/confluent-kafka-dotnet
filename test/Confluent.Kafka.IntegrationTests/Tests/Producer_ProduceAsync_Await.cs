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

#pragma warning disable xUnit1026

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Ensures that awaiting ProduceAsync does not deadlock and
        ///     some other basic things.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_ProduceAsync_Await_Serializing(string bootstrapServers)
        {
            LogToFile("start Producer_ProduceAsync_Await_Serializing");

            Func<Task> mthd = async () =>
            {
                using (var producer = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
                {
                    var dr = await producer.ProduceAsync(
                        singlePartitionTopic,
                        new Message<Null, string> { Value = "test string" });
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                    Assert.NotEqual(Offset.Unset, dr.Offset);
                }
            };

            mthd().Wait();

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_ProduceAsync_Await_Serializing");
        }

        /// <summary>
        ///     Ensures that awaiting ProduceAsync does not deadlock and
        ///     some other basic things (variant 2).
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_ProduceAsync_Await_NonSerializing(string bootstrapServers)
        {
            LogToFile("start Producer_ProduceAsync_Await_NonSerializing");

            using (var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
            {
                var dr = await producer.ProduceAsync(
                    singlePartitionTopic,
                    new Message<byte[], byte[]> { Value = Encoding.UTF8.GetBytes("test string") });
                Assert.NotEqual(Offset.Unset, dr.Offset);
            }

            Assert.Equal(0, Library.HandleCount);

            LogToFile("end   Producer_ProduceAsync_Await_NonSerializing");
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public Task Producer_ProduceAsync_Await_Throws_WithInstrumentation(string bootstrapServers)
        {
            return Producer_ProduceAsync_Await_Throws(bootstrapServers, true);
        }

        /// <summary>
        ///     Ensures that ProduceAsync throws when the DeliveryReport
        ///     has an error (produced to non-existent partition).
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_ProduceAsync_Await_Throws(string bootstrapServers, bool instrument = false)
        {
            LogToFile("start Producer_ProduceAsync_Await_Throws");

            var diagnosticObserver = instrument ? new EventObserverAndRecorder(Diagnostics.DiagnosticListenerName) : null;
            try
            {
                using (var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
                {
                    await Assert.ThrowsAsync<ProduceException<byte[], byte[]>>(
                        async () =>
                        {
                            await producer.ProduceAsync(
                                new TopicPartition(singlePartitionTopic, 42),
                                new Message<byte[], byte[]> { Value = Encoding.UTF8.GetBytes("test string") });
                            throw new Exception("unexpected exception");
                        });
                }

                if (instrument)
                {
                    Assert.Equal(2, diagnosticObserver.Records.Count);
                    Assert.Equal(1, diagnosticObserver.Records.Count(rec => rec.Key.EndsWith("Start")));
                    Assert.Equal(1, diagnosticObserver.Records.Count(rec => rec.Key.EndsWith("Exception")));

                    Assert.True(diagnosticObserver.Records.TryDequeue(out KeyValuePair<string, object> startEvent));
                    Assert.Equal(Diagnostics.Producer.StartName, startEvent.Key);

                    Assert.Equal(singlePartitionTopic, EventObserverAndRecorder.ReadPublicProperty<string>(startEvent.Value, "Topic"));
                    Message<byte[], byte[]> message = EventObserverAndRecorder.ReadPublicProperty<Message<byte[], byte[]>>(startEvent.Value, "Message");
                    Assert.NotNull(message);
                    Assert.Equal("test string", Encoding.UTF8.GetString(message.Value));

                    string startTraceId = message.Headers?.Where(h => h.Key == Diagnostics.TraceParentHeaderName).Select(h => Encoding.UTF8.GetString(h.GetValueBytes())).FirstOrDefault();
                    Assert.NotNull(startTraceId);

                    Assert.True(diagnosticObserver.Records.TryDequeue(out KeyValuePair<string, object> stopEvent));
                    Assert.Equal(Diagnostics.Producer.ExceptionName, stopEvent.Key);
                    Assert.Equal(singlePartitionTopic, EventObserverAndRecorder.ReadPublicProperty<string>(stopEvent.Value, "Topic"));
                    message = EventObserverAndRecorder.ReadPublicProperty<Message<byte[], byte[]>>(stopEvent.Value, "Message");
                    Assert.Equal("test string", Encoding.UTF8.GetString(message.Value));
                    ProduceException<byte[], byte[]> exception = EventObserverAndRecorder.ReadPublicProperty<ProduceException<byte[], byte[]>>(stopEvent.Value, "Exception");
                    Assert.NotNull(exception);
                    Assert.Equal(ErrorCode.Local_UnknownPartition, exception.Error.Code);

                    string stopTraceId = message.Headers?.Where(h => h.Key == Diagnostics.TraceParentHeaderName).Select(h => Encoding.UTF8.GetString(h.GetValueBytes())).FirstOrDefault();
                    Assert.Equal(startTraceId, stopTraceId);
                }

                // variation 2

                Func<Task> mthd = async () =>
                {
                    using (var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
                    {
                        var dr = await producer.ProduceAsync(
                            new TopicPartition(singlePartitionTopic, 1001),
                            new Message<byte[], byte[]> { Value = Encoding.UTF8.GetBytes("test string") });
                        throw new Exception("unexpected exception.");
                    }
                };

                Assert.Throws<AggregateException>(() => { mthd().Wait(); });

                Assert.Equal(0, Library.HandleCount);
            }
            finally
            {
                diagnosticObserver?.Dispose();
            }

            LogToFile("end   Producer_ProduceAsync_Await_Throws");
        }
    }
}
