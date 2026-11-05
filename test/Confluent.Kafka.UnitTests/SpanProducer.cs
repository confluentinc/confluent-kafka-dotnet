// Copyright 2026 Confluent Inc.
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
using Xunit;


namespace Confluent.Kafka.UnitTests
{
    public class SpanProducerTests
    {
        [Fact]
        public void SpanProduce_ExtensionMethodAvailable()
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
            using var producer = new ProducerBuilder<string, byte[]>(config).Build();

            var key = Encoding.UTF8.GetBytes("key");
            var value = Encoding.UTF8.GetBytes("value");

            // Extension method should be callable. The produce itself may fail
            // (no broker) but it should not throw NotSupportedException.
            var ex = Record.Exception(() =>
                producer.Produce("test-topic", (ReadOnlySpan<byte>)key, (ReadOnlySpan<byte>)value));

            if (ex != null)
            {
                Assert.IsNotType<NotSupportedException>(ex);
            }
        }

        [Fact]
        public void SpanProduce_DependentProducer_ExtensionMethodAvailable()
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
            using var owner = new ProducerBuilder<byte[], byte[]>(config).Build();
            using var dependent = new DependentProducerBuilder<string, string>(owner.Handle).Build();

            var key = Encoding.UTF8.GetBytes("key");
            var value = Encoding.UTF8.GetBytes("value");

            var ex = Record.Exception(() =>
                dependent.Produce("test-topic", (ReadOnlySpan<byte>)key, (ReadOnlySpan<byte>)value));

            if (ex != null)
            {
                Assert.IsNotType<NotSupportedException>(ex);
            }
        }

        [Fact]
        public void SpanProduce_ThrowsWhenDeliveryReportsDisabled()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                EnableDeliveryReports = false
            };
            using var producer = new ProducerBuilder<byte[], byte[]>(config).Build();

            var key = Encoding.UTF8.GetBytes("key");
            var value = Encoding.UTF8.GetBytes("value");

            Assert.Throws<InvalidOperationException>(() =>
                producer.Produce("test-topic", (ReadOnlySpan<byte>)key, (ReadOnlySpan<byte>)value, _ => { }));
        }

        [Fact]
        public void SpanProduce_NoExceptionWithoutDeliveryHandler()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                EnableDeliveryReports = false
            };
            using var producer = new ProducerBuilder<byte[], byte[]>(config).Build();

            var key = Encoding.UTF8.GetBytes("key");
            var value = Encoding.UTF8.GetBytes("value");

            // Should not throw — no delivery handler, so delivery reports being
            // disabled is irrelevant. The produce itself may fail (no broker),
            // but should throw KafkaException, not InvalidOperationException.
            var ex = Record.Exception(() =>
                producer.Produce("test-topic", (ReadOnlySpan<byte>)key, (ReadOnlySpan<byte>)value));

            if (ex != null)
            {
                Assert.IsNotType<InvalidOperationException>(ex);
            }
        }

        [Fact]
        public void SpanProduce_InvalidTimestampThrows()
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
            using var producer = new ProducerBuilder<byte[], byte[]>(config).Build();

            var key = Encoding.UTF8.GetBytes("key");
            var value = Encoding.UTF8.GetBytes("value");
            var badTimestamp = new Timestamp(DateTime.UtcNow, TimestampType.LogAppendTime);

            Assert.Throws<ArgumentException>(() =>
                producer.Produce(
                    new TopicPartition("test-topic", Partition.Any),
                    (ReadOnlySpan<byte>)key, (ReadOnlySpan<byte>)value, badTimestamp, null));
        }

        [Fact]
        public void SpanProduce_EmptySpansAreAccepted()
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
            using var producer = new ProducerBuilder<byte[], byte[]>(config).Build();

            // Empty spans (null key/value) should not throw validation errors.
            // The produce itself may fail (no broker) but not with ArgumentException.
            var ex = Record.Exception(() =>
                producer.Produce("test-topic", ReadOnlySpan<byte>.Empty, ReadOnlySpan<byte>.Empty));

            if (ex != null)
            {
                Assert.IsNotType<ArgumentException>(ex);
            }
        }
    }
}
