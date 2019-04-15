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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test more concurrent ProduceAsync requests than
    ///     there are thread pool worker threads.
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_ProduceAsync_HighConcurrency(string bootstrapServers)
        {
            LogToFile("start Producer_ProduceAsync_HighConcurrency");

            ThreadPool.GetMaxThreads(out int originalWorkerThreads, out int originalCompletionPortThreads);

            ThreadPool.GetMinThreads(out int workerThreads, out int completionPortThreads);   
            ThreadPool.SetMaxThreads(workerThreads, completionPortThreads);
            ThreadPool.GetMaxThreads(out workerThreads, out completionPortThreads);

            var pConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            using (var tempTopic = new TemporaryTopic(bootstrapServers, 1))
            using (var producer = new ProducerBuilder<Null, string>(pConfig)
                .SetValueSerializer(new SimpleAsyncSerializer().SyncOverAsync())
                .Build())
            using (var dProducer = new DependentProducerBuilder<Null, string>(producer.Handle)
                .SetValueSerializer(new SimpleAsyncSerializer())
                .Build())
            {
                var tasks = new List<Task>();

                int N = workerThreads+2;
                for (int i=0; i<N; ++i)
                {
                    tasks.Add(producer.ProduceAsync(tempTopic.Name, new Message<Null, string> { Value = "test" }));
                }

                Task.WaitAll(tasks.ToArray());

                for (int i=0; i<N; ++i)
                {
                    tasks.Add(dProducer.ProduceAsync(tempTopic.Name, new Message<Null, string> { Value = "test" }));
                }

                Task.WaitAll(tasks.ToArray());
            }

            ThreadPool.SetMaxThreads(originalWorkerThreads, originalCompletionPortThreads);

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_ProduceAsync_HighConcurrency");
        }
    }
}
