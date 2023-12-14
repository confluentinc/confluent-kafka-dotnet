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
using System.Threading;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    /// Test multiple calls to SetLogHandler, SetStatisticsHandler and SetErrorHandler
    /// </summary>
    public partial class Tests
    {
        private const string UnreachableBootstrapServers = "localhost:9000";
        
        [Theory, MemberData(nameof(KafkaParameters))]
        public void ProducerBuilder_SetLogHandler(string bootstrapServers)
        {
            LogToFile("start ProducerBuilder_SetLogHandler");

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                Debug = "all"
            };

            ManualResetEventSlim mres1 = new(), mres2 = new();

            using var _ = new ProducerBuilder<string, string>(producerConfig)
                       .SetLogHandler((_, _) => mres1.Set())
                       .SetLogHandler((_, _) => mres2.Set())
                       .Build();
            
            Assert.True(mres1.Wait(TimeSpan.FromSeconds(5)));
            Assert.True(mres2.Wait(TimeSpan.FromSeconds(5)));

            LogToFile("end   ProducerBuilder_SetLogHandler");
        }
        
        [Theory, MemberData(nameof(KafkaParameters))]
        public void ProducerBuilder_SetStatisticsHandler(string bootstrapServers)
        {
            LogToFile("start ProducerBuilder_SetStatisticsHandler");

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                StatisticsIntervalMs = 100
            };

            ManualResetEventSlim mres1 = new(), mres2 = new();

            using var _ = new ProducerBuilder<string, string>(producerConfig)
                .SetStatisticsHandler((_, _) => mres1.Set())
                .SetStatisticsHandler((_, _) => mres2.Set())
                .Build();
            
            Assert.True(mres1.Wait(TimeSpan.FromSeconds(5)));
            Assert.True(mres2.Wait(TimeSpan.FromSeconds(5)));

            LogToFile("end   ProducerBuilder_SetStatisticsHandler");
        }
        
        [Theory, InlineData(UnreachableBootstrapServers)]
        public void ProducerBuilder_SetErrorHandler(string bootstrapServers)
        {
            LogToFile("start ProducerBuilder_SetErrorHandler");

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers
            };

            ManualResetEventSlim mres1 = new(), mres2 = new();

            using var _ = new ProducerBuilder<string, string>(producerConfig)
                .SetErrorHandler((_, _) => mres1.Set())
                .SetErrorHandler((_, _) => mres2.Set())
                .Build();
            
            Assert.True(mres1.Wait(TimeSpan.FromSeconds(5)));
            Assert.True(mres2.Wait(TimeSpan.FromSeconds(5)));

            LogToFile("end   ProducerBuilder_SetErrorHandler");
        }
        
        [Theory, MemberData(nameof(KafkaParameters))]
        public void ConsumerBuilder_SetLogHandler(string bootstrapServers)
        {
            LogToFile("start ConsumerBuilder_SetLogHandler");
            
            int N = 2;
            var firstProduced = Util.ProduceNullStringMessages(bootstrapServers, singlePartitionTopic, 100, N);
            
            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000,
                EnablePartitionEof = true,
                Debug = "all"
            };

            ManualResetEventSlim mres1 = new(), mres2 = new();

            using var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    Assert.Single(partitions);
                    Assert.Equal(firstProduced.TopicPartition, partitions[0]);
                    return partitions.Select(p => new TopicPartitionOffset(p, firstProduced.Offset));
                })
                .SetLogHandler((_, _) => mres1.Set())
                .SetLogHandler((_, _) => mres2.Set())
                .Build();
            consumer.Subscribe(singlePartitionTopic);

            Assert.True(mres1.Wait(TimeSpan.FromSeconds(5)));
            Assert.True(mres2.Wait(TimeSpan.FromSeconds(5)));

            LogToFile("end   ConsumerBuilder_SetLogHandler");
        }
        
        [Theory, MemberData(nameof(KafkaParameters))]
        public void ConsumerBuilder_SetStatisticsHandler(string bootstrapServers)
        {
            LogToFile("start ConsumerBuilder_SetStatisticsHandler");
            
            int N = 2;
            var firstProduced = Util.ProduceNullStringMessages(bootstrapServers, singlePartitionTopic, 100, N);
            
            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000,
                EnablePartitionEof = true,
                StatisticsIntervalMs = 100
            };

            ManualResetEventSlim mres1 = new(), mres2 = new();

            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                       .SetPartitionsAssignedHandler((c, partitions) =>
                       {
                           Assert.Single(partitions);
                           Assert.Equal(firstProduced.TopicPartition, partitions[0]);
                           return partitions.Select(p => new TopicPartitionOffset(p, firstProduced.Offset));
                       })
                       .SetStatisticsHandler((_, _) => mres1.Set())
                       .SetStatisticsHandler((_, _) => mres2.Set())
                       .Build())
            {
                consumer.Subscribe(singlePartitionTopic);
                
                int msgCnt = 0;
                while (true)
                {
                    var record = consumer.Consume(TimeSpan.FromMilliseconds(100));
                    if (record == null) { continue; }
                    if (record.IsPartitionEOF) { break; }
                    msgCnt += 1;
                }

                Assert.True(mres1.Wait(TimeSpan.FromSeconds(5)));
                Assert.True(mres2.Wait(TimeSpan.FromSeconds(5)));
                consumer.Close();
            }

            LogToFile("end   ConsumerBuilder_SetStatisticsHandler");
        }
        
        [SkipWhenCITheory("Requires to stop the broker in the while loop to simulate broker is down."), MemberData(nameof(KafkaParameters))]
        public void ConsumerBuilder_SetErrorHandler(string bootstrapServers)
        {
            LogToFile("start ConsumerBuilder_SetErrorHandler");
            
            int N = 2;
            var firstProduced = Util.ProduceNullStringMessages(bootstrapServers, singlePartitionTopic, 100, N);
            
            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000
            };

            bool errorHandler1Called = false, errorHandler2Called = false;

            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                       .SetPartitionsAssignedHandler((c, partitions) =>
                       {
                           Assert.Single(partitions);
                           Assert.Equal(firstProduced.TopicPartition, partitions[0]);
                           return partitions.Select(p => new TopicPartitionOffset(p, firstProduced.Offset));
                       })
                       .SetErrorHandler((_, _) => errorHandler1Called = true)
                       .SetErrorHandler((_, _) => errorHandler2Called = true)
                       .Build())
            {
                consumer.Subscribe(singlePartitionTopic);
                
                int msgCnt = 0;
                while (!errorHandler1Called && !errorHandler2Called)
                {
                    var record = consumer.Consume(TimeSpan.FromMilliseconds(100));
                    if (record == null) { continue; }
                    msgCnt += 1;
                }

                consumer.Close();
            }

            LogToFile("end   ConsumerBuilder_SetErrorHandler");
        }
    }
}
