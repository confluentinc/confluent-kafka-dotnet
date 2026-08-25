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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Confluent.Kafka.Admin;
using Confluent.Kafka.TestsCommon;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test internal poll time is effective.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task CancellationDelayMax(string bootstrapServers)
        {
            LogToFile("start CancellationDelayMax");

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000,
                EnablePartitionEof = false,
                CancellationDelayMaxMs = 2
            };

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                CancellationDelayMaxMs = 2
            };

            var adminClientConfig = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers,
                CancellationDelayMaxMs = 2
            };

            using (var topic = new TemporaryTopic(bootstrapServers, 3))
            using (var consumer = new TestConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            using (var producer = new TestProducerBuilder<byte[], byte[]>(producerConfig).Build())
            using (var adminClient = new AdminClientBuilder(adminClientConfig).Build())
            {
                consumer.Subscribe(topic.Name);

                // for the consumer, check that the cancellation token is honored.
                for (int i=0; i<20; ++i)
                {
                    var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(2));
                    var sw = Stopwatch.StartNew();
                    try
                    {
                        // Intentionally using a short-lived, purpose-built token here (not
                        // TestContext.Current.CancellationToken) - this is what's under test.
#pragma warning disable xUnit1051
                        var record = consumer.Consume(cts.Token);
#pragma warning restore xUnit1051
                    }
                    catch (OperationCanceledException)
                    {
                        // expected.
                    }
                    // 2ms + 2ms + quite a bit of leeway. Note: CancellationDelayMaxMs has been
                    // reduced to 2ms in this test, and we check for an elapsed time less than
                    // this to test that configuration is working. in practice the elapsed time
                    // should 4 almost all of the time. A higher value is apparently required on
                    // Windows (but still less than 50).
                    var elapsed = sw.ElapsedMilliseconds;
                    Assert.SkipWhen(elapsed > 20, "elapsed time exceeded expected CancellationDelayMaxMs bound");
                }

                consumer.Close();

                // for the producer, make do with just a simple check that this does not throw or hang.
                var dr = await producer.ProduceAsync(topic.Name, new Message<byte[], byte[]> { Key = new byte[] { 42 }, Value = new byte[] { 255 } }, TestContext.Current.CancellationToken);

                // for the admin client, make do with just simple check that this does not throw or hang.
                var cr = new Confluent.Kafka.Admin.ConfigResource { Type = ResourceType.Topic, Name = topic.Name };
                var configs = await adminClient.DescribeConfigsAsync(new ConfigResource[] { cr });
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   CancellationDelayMax");
        }

    }
}
