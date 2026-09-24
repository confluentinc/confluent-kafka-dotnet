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
using System.Threading.Tasks;
using Xunit;


namespace Confluent.Kafka.UnitTests
{
    /// <summary>
    ///     Tests that the asynchronous cluster id resolution handed to serdes keeps
    ///     at most one call into librdkafka in flight per handle, and does not
    ///     cache its outcome once that call completes.
    /// </summary>
    public class ClusterIdResolutionTests
    {
        // No broker listens here, so a resolution waits for its full timeout and
        // then completes with null.
        private static ProducerConfig UnreachableProducerConfig()
            => new ProducerConfig { BootstrapServers = "localhost:1" };

        [Fact]
        public async Task ConcurrentResolutions_ShareASingleCallInFlight()
        {
            using (var producer = new ProducerBuilder<Null, string>(UnreachableProducerConfig()).Build())
            {
                var handle = producer.Handle.LibrdkafkaHandle;

                var first = handle.ClusterIdAsync(500);
                var second = handle.ClusterIdAsync(500);

                Assert.Same(first, second);
                Assert.Null(await first);
            }
        }

        [Fact]
        public async Task ACompletedResolution_IsNotCached()
        {
            using (var producer = new ProducerBuilder<Null, string>(UnreachableProducerConfig()).Build())
            {
                var handle = producer.Handle.LibrdkafkaHandle;

                var first = handle.ClusterIdAsync(200);
                Assert.Null(await first);

                // The failed attempt is not handed back: the next caller starts a
                // fresh one.
                var retry = handle.ClusterIdAsync(200);
                Assert.NotSame(first, retry);
                Assert.Null(await retry);
            }
        }

        [Fact]
        public async Task ADependentProducer_SharesTheCallInFlight()
        {
            using (var producer = new ProducerBuilder<Null, string>(UnreachableProducerConfig()).Build())
            using (var dependent = new DependentProducerBuilder<Null, string>(producer.Handle).Build())
            {
                var first = producer.Handle.LibrdkafkaHandle.ClusterIdAsync(500);
                var second = dependent.Handle.LibrdkafkaHandle.ClusterIdAsync(500);

                Assert.Same(first, second);
                Assert.Null(await first);
            }
        }

        [Fact]
        public async Task Resolution_FailsOnceTheClientIsDisposed()
        {
            var producer = new ProducerBuilder<Null, string>(UnreachableProducerConfig()).Build();
            var handle = producer.Handle.LibrdkafkaHandle;
            producer.Dispose();

            await Assert.ThrowsAsync<ObjectDisposedException>(
                async () => await handle.ClusterIdAsync(500));
        }
    }
}
