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
    ///     Test Producer.Poll when producer in automatic poll mode.
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_Poll(string bootstrapServers)
        {
            LogToFile("start Producer_Poll");

            using (var tempTopic = new TemporaryTopic(bootstrapServers, 1))
            using (var producer = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
            {
                var r = producer.ProduceAsync(tempTopic.Name, new Message<Null, string> { Value = "a message" }).Result;
                Assert.True(r.Status == PersistenceStatus.Persisted);

                // should be no events to serve and this should block for 500ms.
                var sw = new Stopwatch();
                sw.Start();
                // Note: Poll returns the number of events served since the last
                // call to Poll (or if the poll method hasn't been called, over
                // the lifetime of the producer).
                Assert.True(producer.Poll(TimeSpan.FromMilliseconds(500)) >= 1);
                var elapsed = sw.ElapsedMilliseconds;
                Assert.True(elapsed < 2);
                Assert.Equal(0, producer.Poll(TimeSpan.FromMilliseconds(500)));
                Assert.True(sw.ElapsedMilliseconds >= 500);

                sw.Reset();
                sw.Start();
                producer.Produce(tempTopic.Name, new Message<Null, string> { Value = "a message 2" }, dr => Assert.False(dr.Error.IsError));
                // should block until the callback for the Produce call executes.
                Assert.Equal(1, producer.Poll(TimeSpan.FromSeconds(4)));
                Assert.True(sw.ElapsedMilliseconds > 0);
                Assert.True(sw.ElapsedMilliseconds < 3500);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_Poll");
        }
    }
}
