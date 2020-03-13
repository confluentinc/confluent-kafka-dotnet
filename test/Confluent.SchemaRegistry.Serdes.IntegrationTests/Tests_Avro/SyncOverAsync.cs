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
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Xunit;


namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    /// <summary>
    ///     Test sync over async does not deadlock when number
    ///     of threads blocked on is one less than the number
    ///     of worker threads available.
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(TestParameters))]
        public static void SyncOverAsync(string bootstrapServers, string schemaRegistryServers)
        {
            ThreadPool.GetMaxThreads(out int originalWorkerThreads, out int originalCompletionPortThreads);

            ThreadPool.GetMinThreads(out int workerThreads, out int completionPortThreads);   
            ThreadPool.SetMaxThreads(workerThreads, completionPortThreads);
            ThreadPool.GetMaxThreads(out workerThreads, out completionPortThreads);

            var pConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers
            };
            
            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryServers
            };

            var topic = Guid.NewGuid().ToString();
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer = new ProducerBuilder<Null, string>(pConfig)
                .SetValueSerializer(new AvroSerializer<string>(schemaRegistry).AsSyncOverAsync())
                .Build())
            {
                var tasks = new List<Task>();

                // will deadlock if N >= workerThreads. Set to max number that 
                // should not deadlock.
                int N = workerThreads-1;
                for (int i=0; i<N; ++i)
                {
                    Func<int, Action> actionCreator = (taskNumber) =>
                    {
                        return () =>
                        {
                            object waitObj = new object();

                            Action<DeliveryReport<Null, string>> handler = dr => 
                            {
                                Assert.True(dr.Error.Code == ErrorCode.NoError);

                                lock (waitObj)
                                {
                                    Monitor.Pulse(waitObj);
                                }
                            };

                            producer.Produce(topic, new Message<Null, string> { Value = $"value: {taskNumber}" }, handler);

                            lock (waitObj)
                            {
                                Monitor.Wait(waitObj);
                            }
                        };
                    };

                    tasks.Add(Task.Run(actionCreator(i)));
                }

                Task.WaitAll(tasks.ToArray());
            }

            ThreadPool.SetMaxThreads(originalWorkerThreads, originalCompletionPortThreads);

            Assert.Equal(0, Library.HandleCount);
        }
    }
}
