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
using System.Linq;
using System.Text;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Confluent.Kafka.Admin;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test internal poll time is effective.
        /// </summary>
        [SkippableTheory, MemberData(nameof(KafkaParameters))]
        public static void CancellationDelayMax(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
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
            using (var consumer = new Consumer(consumerConfig))
            using (var producer = new Producer(producerConfig))
            using (var adminClient = new AdminClient(adminClientConfig))
            {
                consumer.Subscribe(topic.Name);

                // for the consumer, check that the cancellation token is honored.
                for (int i=0; i<20; ++i)
                {
                    var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(2));
                    var sw = Stopwatch.StartNew();
                    try
                    {
                        var record = consumer.Consume(cts.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        // expected.
                    }
                    // 2ms + 2ms + quite a bit of leeway (but still much less than the default of 50).
                    // in practice this should 4 almost all of the time.
                    var elapsed = sw.ElapsedMilliseconds;
                    Skip.If(elapsed > 8);
                }

                consumer.Close();

                // for the producer, make do with just a simple check that this does not throw or hang.
                var dr = producer.ProduceAsync(topic.Name, new Message { Key = new byte[] { 42 }, Value = new byte[] { 255 } }).Result;
                
                // for the admin client, make do with just simple check that this does not throw or hang.
                var cr = new Confluent.Kafka.Admin.ConfigResource { Type = ResourceType.Topic, Name = topic.Name };
                var configs = adminClient.DescribeConfigsAsync(new ConfigResource[] { cr }).Result;
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   CancellationDelayMax");
        }

    }
}
