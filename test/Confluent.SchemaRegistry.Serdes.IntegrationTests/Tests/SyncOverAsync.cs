// Copyright 2018 Confluent Inc.
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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Examples.AvroSpecific;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;
using Avro;
using Avro.Generic;
using Xunit;


namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test that use of SyncOverAsync wrappers doesn't deadlock due to
        ///     thread pool exhaustion or other reasons.
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void SyncOverAsync(string bootstrapServers, string schemaRegistryServers)
        {
            ThreadPool.GetMinThreads(out int workerThreads, out int completionPortThreads);   
            ThreadPool.SetMaxThreads(workerThreads, completionPortThreads);
            ThreadPool.GetMaxThreads(out workerThreads, out completionPortThreads);
            // Make sure this is something reasonably low. If these fail for some reason, may want to reconsider test.
            Assert.True(workerThreads < 128);
            Assert.True(completionPortThreads < 128);
            File.AppendAllLines("c:\\tmp\\deadlock.txt", new [] { $"A {workerThreads} {completionPortThreads}\n" });

            var pConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            var srConfig = new SchemaRegistryConfig { SchemaRegistryUrl = schemaRegistryServers };
            
            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            using (var schemaRegistry = new CachedSchemaRegistryClient(srConfig))
            using (var producer = new ProducerBuilder<Null, User>(pConfig)
                .SetValueSerializer(new AsyncAvroSerializer<User>(schemaRegistry).AsSyncOverAsync())
                .Build())
            {
                Action<DeliveryReport<Null, User>> handler = dr =>
                {
                    Assert.True(dr.Error.Code == ErrorCode.NoError);
                };

                var tasks = new List<Task>();
                for (int i=0; i<workerThreads-4; ++i)
                {
                    tasks.Add(Task.Run(() => producer.BeginProduce(topic.Name, new Message<Null, User> { Value = new User { name = $"name-{i}", favorite_color = $"col-{i}",  favorite_number = i } }, handler)));
                }
                ThreadPool.GetAvailableThreads(out workerThreads, out completionPortThreads);
                // at this point N SR requests will be in flight where N > num workerThreads.
                // Assert.Equal(0, workerThreads);
                File.AppendAllLines("c:\\tmp\\deadlock.txt", new [] { $"B {workerThreads} {completionPortThreads}\n" });
            
                producer.Flush();

                // the primary test here is that the integration test doesn't deadlock
            }
        }

    }
}
