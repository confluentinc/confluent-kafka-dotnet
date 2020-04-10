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

#pragma warning disable xUnit1026

using System;
using System.Text;
using System.Threading.Tasks;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {

        class TestSerializer : IAsyncSerializer<string>
        {
            public Task<byte[]> SerializeAsync(string data, SerializationContext context)
            {
                Assert.NotNull(context.Headers);
                context.Headers.Add("test_header", new byte[] { 100, 42 });
                return Task.FromResult(Encoding.UTF8.GetBytes("test_value"));
            }
        }

        class TestDeserializer : IDeserializer<string>
        {
            public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            {
                Assert.NotNull(context.Headers);
                return null;
            }
        }


        /// <summary>
        ///     Test population of Headers property in SerializationContext.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void HeadersSerializationContext(string bootstrapServers)
        {
            LogToFile("start Headers_SerializationContext");

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                EnableIdempotence = true
            };

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000
            };


            // Test Headers property is not null in Serializer, and that header added there is produced.
            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            using (var producer = new ProducerBuilder<Null, string>(producerConfig)
                .SetValueSerializer(new TestSerializer())
                .Build())
            using (var consumer = new ConsumerBuilder<Null, string>(consumerConfig)
                .SetValueDeserializer(new TestDeserializer())
                .Build())
            {
                producer.ProduceAsync(topic.Name, new Message<Null, string> { Value = "aaa" });
                consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));
                var cr = consumer.Consume();
                Assert.NotNull(cr.Message);
                Assert.Single(cr.Message.Headers);
                cr.Message.Headers.TryGetLastBytes("test_header", out byte[] testHeader);
                Assert.Equal(new byte[] { 100, 42 }, testHeader);
            }

            
            // Test accumulation of headers
            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            using (var producer = new ProducerBuilder<string, string>(producerConfig)
                .SetKeySerializer(new TestSerializer())
                .SetValueSerializer(new TestSerializer())
                .Build())
            using (var consumer = new ConsumerBuilder<string, string>(consumerConfig)
                .SetKeyDeserializer(new TestDeserializer())
                .SetValueDeserializer(new TestDeserializer())
                .Build())
            {
                producer.ProduceAsync(topic.Name, new Message<string, string> { Value = "aaa", Headers = new Headers { new Header("original", new byte[] { 32 }) } });
                consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));
                var cr = consumer.Consume();
                Assert.NotNull(cr.Message);
                Assert.Equal(3, cr.Message.Headers.Count);
                cr.Message.Headers.TryGetLastBytes("test_header", out byte[] testHeader);
                Assert.Equal(new byte[] { 100, 42 }, testHeader);
                cr.Message.Headers.TryGetLastBytes("original", out byte[] originalHeader);
                Assert.Equal(new byte[] { 32 }, originalHeader);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Headers_SerializationContext");
        }
    }
}