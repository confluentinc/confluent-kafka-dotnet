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
using System.Diagnostics;
using System.Threading.Tasks;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_Poll_Backoff(string bootstrapServers)
        {
            LogToFile("start Producer_Poll_Backoff");
            bool skipFlakyTests = semaphoreSkipFlakyTests();
            if (skipFlakyTests)
            {
                LogToFile("Skipping Producer_Poll_Backoff Test on Semaphore due to its flaky nature");
                return;
            }
            var pConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                QueueBufferingMaxMessages = 10,
                LingerMs = 100
            };

            using (var tempTopic = new TemporaryTopic(bootstrapServers, 1))
            using (var producer = new ProducerBuilder<Null, string>(pConfig).Build())
            {
                // test timing around producer.Poll.
                Stopwatch sw = new Stopwatch();
                sw.Start();
                var exceptionCount = 0;
                for (int i=0; i<11; ++i)
                {
                    try
                    {
                        producer.Produce(tempTopic.Name, new Message<Null, string> { Value = "a message" });
                    }
                    catch (ProduceException<Null, string> ex)
                    {
                        exceptionCount += 1;
                        Assert.Equal(ErrorCode.Local_QueueFull, ex.Error.Code);
                        var served = producer.Poll(TimeSpan.FromSeconds(4));
                        Assert.True(served >= 1);
                        var elapsed = sw.ElapsedMilliseconds;
                        Assert.True(elapsed > 100); // linger.ms
                        Assert.True(elapsed < 4000);
                    }
                }
                Assert.Equal(1, exceptionCount);

                producer.Flush();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_Poll_Backoff");
        }

    }
}
