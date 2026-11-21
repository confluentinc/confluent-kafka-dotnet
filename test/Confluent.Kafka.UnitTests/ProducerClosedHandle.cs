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
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;


namespace Confluent.Kafka.UnitTests
{
    /// <summary>
    ///     Producer methods called after Dispose must throw ObjectDisposedException
    ///     instead of passing the destroyed librdkafka handle to native code.
    /// </summary>
    public class ProducerClosedHandleTests
    {
        private static IProducer<byte[], byte[]> DisposedProducer(string transactionalId = null)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                TransactionalId = transactionalId
            };
            var producer = new ProducerBuilder<byte[], byte[]>(config).Build();
            producer.Dispose();
            return producer;
        }

        [Fact]
        public void Produce_AfterDispose_ThrowsObjectDisposedException()
        {
            var producer = DisposedProducer();

            Assert.Throws<ObjectDisposedException>(() =>
                producer.Produce("test-topic", new Message<byte[], byte[]> { Value = new byte[] { 1, 2, 3 } }));
        }

        [Fact]
        public async Task ProduceAsync_AfterDispose_ThrowsObjectDisposedException()
        {
            var producer = DisposedProducer();

            await Assert.ThrowsAsync<ObjectDisposedException>(() =>
                producer.ProduceAsync("test-topic", new Message<byte[], byte[]> { Value = new byte[] { 1, 2, 3 } }));
        }

        [Fact]
        public void TransactionMethods_AfterDispose_ThrowObjectDisposedException()
        {
            var producer = DisposedProducer("closed-handle-test");

            Assert.Throws<ObjectDisposedException>(() => producer.InitTransactions(TimeSpan.FromSeconds(1)));
            Assert.Throws<ObjectDisposedException>(() => producer.BeginTransaction());
            Assert.Throws<ObjectDisposedException>(() => producer.CommitTransaction(TimeSpan.FromSeconds(1)));
            Assert.Throws<ObjectDisposedException>(() => producer.AbortTransaction(TimeSpan.FromSeconds(1)));

            var consumerConfig = new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "closed-handle-test" };
            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                var groupMetadata = consumer.ConsumerGroupMetadata;
                Assert.Throws<ObjectDisposedException>(() => producer.SendOffsetsToTransaction(
                    new List<TopicPartitionOffset> { new TopicPartitionOffset("test-topic", 0, 1) },
                    groupMetadata,
                    TimeSpan.FromSeconds(1)));
            }
        }
    }
}
