// Copyright 2019 Confluent Inc.
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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test sync over async does not deadlock when number
    ///     of threads blocked on is one less than the number
    ///     of worker threads available.
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_Produce_SyncOverAsync(string bootstrapServers)
        {
            LogToFile("start Producer_Produce_SyncOverAsync");

            ThreadPool.GetMaxThreads(out int originalWorkerThreads, out int originalCompletionPortThreads);

            ThreadPool.GetMinThreads(out int workerThreads, out int completionPortThreads);   
            ThreadPool.SetMaxThreads(workerThreads, completionPortThreads);
            ThreadPool.GetMaxThreads(out workerThreads, out completionPortThreads);

            var pConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers
            };
            
            using (var tempTopic = new TemporaryTopic(bootstrapServers, 1))
            using (var producer = new ProducerBuilder<Null, string>(pConfig)
                .SetValueSerializer(new SimpleAsyncSerializer().SyncOverAsync())
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

                            producer.Produce(tempTopic.Name, new Message<Null, string> { Value = $"value: {taskNumber}" }, handler);

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
            LogToFile("end   Producer_Produce_SyncOverAsync");
        }
    }
}
