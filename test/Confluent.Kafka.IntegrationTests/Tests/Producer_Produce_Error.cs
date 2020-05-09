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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Tests <see cref="Producer.Produce" /> error cases.
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_Produce_Error_WithInstrumentation(string bootstrapServers)
        {
            Producer_Produce_Error(bootstrapServers, true);
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_Produce_Error(string bootstrapServers, bool instrument = false)
        {
            LogToFile("start Producer_Produce_Error");

            var diagnosticObserver = instrument ? new EventObserverAndRecorder(Diagnostics.DiagnosticListenerName) : null;
            try
            {
                var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

                // serializer case.

                int count = 0;
                Action<DeliveryReport<Null, String>> dh = (DeliveryReport<Null, String> dr) =>
                {
                    Assert.Equal(ErrorCode.Local_UnknownPartition, dr.Error.Code);
                    Assert.False(dr.Error.IsFatal);
                    Assert.Equal((Partition)1, dr.Partition);
                    Assert.Equal(singlePartitionTopic, dr.Topic);
                    Assert.Equal(Offset.Unset, dr.Offset);
                    Assert.Null(dr.Message.Key);
                    Assert.Equal("test", dr.Message.Value);
                    Assert.Equal(PersistenceStatus.NotPersisted, dr.Status);
                    Assert.Equal(TimestampType.NotAvailable, dr.Message.Timestamp.Type);
                    count += 1;
                };

                using (var producer =
                    new ProducerBuilder<Null, String>(producerConfig)
                        .SetKeySerializer(Serializers.Null)
                        .SetValueSerializer(Serializers.Utf8)
                        .Build())
                {
                    producer.Produce(new TopicPartition(singlePartitionTopic, 1), new Message<Null, String> { Value = "test" }, dh);
                    producer.Flush(TimeSpan.FromSeconds(10));
                }

                Assert.Equal(1, count);

                if (instrument)
                {
                    Assert.Equal(2, diagnosticObserver.Records.Count);
                    Assert.Equal(1, diagnosticObserver.Records.Count(rec => rec.Key.EndsWith("Start")));
                    Assert.Equal(1, diagnosticObserver.Records.Count(rec => rec.Key.EndsWith("Exception")));

                    Assert.True(diagnosticObserver.Records.TryDequeue(out KeyValuePair<string, object> startEvent));
                    Assert.Equal(Diagnostics.Producer.StartName, startEvent.Key);

                    Assert.Equal(singlePartitionTopic, EventObserverAndRecorder.ReadPublicProperty<string>(startEvent.Value, "Topic"));
                    Message<Null, string> message = EventObserverAndRecorder.ReadPublicProperty<Message<Null, string>>(startEvent.Value, "Message");
                    Assert.NotNull(message);
                    Assert.Equal("test", message.Value);

                    string startTraceId = message.Headers?.Where(h => h.Key == Diagnostics.TraceParentHeaderName).Select(h => Encoding.UTF8.GetString(h.GetValueBytes())).FirstOrDefault();
                    Assert.NotNull(startTraceId);

                    Assert.True(diagnosticObserver.Records.TryDequeue(out KeyValuePair<string, object> stopEvent));
                    Assert.Equal(Diagnostics.Producer.ExceptionName, stopEvent.Key);
                    Assert.Equal(singlePartitionTopic, EventObserverAndRecorder.ReadPublicProperty<string>(stopEvent.Value, "Topic"));
                    message = EventObserverAndRecorder.ReadPublicProperty<Message<Null, string>>(stopEvent.Value, "Message");
                    Assert.Equal("test", message.Value);
                    ProduceException<Null, string> exception = EventObserverAndRecorder.ReadPublicProperty<ProduceException<Null, string>>(stopEvent.Value, "Exception");
                    Assert.NotNull(exception);
                    Assert.Equal(ErrorCode.Local_UnknownPartition, exception.Error.Code);

                    string stopTraceId = message.Headers?.Where(h => h.Key == Diagnostics.TraceParentHeaderName).Select(h => Encoding.UTF8.GetString(h.GetValueBytes())).FirstOrDefault();
                    Assert.Equal(startTraceId, stopTraceId);
                }

                // byte[] case.

                count = 0;
                Action<DeliveryReport<byte[], byte[]>> dh2 = (DeliveryReport<byte[], byte[]> dr) =>
                {
                    Assert.Equal(ErrorCode.Local_UnknownPartition, dr.Error.Code);
                    Assert.Equal((Partition)42, dr.Partition);
                    Assert.Equal(singlePartitionTopic, dr.Topic);
                    Assert.Equal(Offset.Unset, dr.Offset);
                    Assert.Equal(new byte[] { 11 }, dr.Message.Key);
                    Assert.Null(dr.Message.Value);
                    Assert.Equal(TimestampType.NotAvailable, dr.Message.Timestamp.Type);
                    count += 1;
                };

                using (var producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build())
                {
                    producer.Produce(new TopicPartition(singlePartitionTopic, 42), new Message<byte[], byte[]> { Key = new byte[] { 11 } }, dh2);
                    producer.Flush(TimeSpan.FromSeconds(10));
                }

                Assert.Equal(1, count);

                Assert.Equal(0, Library.HandleCount);
            }
            finally
            {
                diagnosticObserver?.Dispose();
            }

            LogToFile("end   Producer_Produce_Error");
        }
    }
}
