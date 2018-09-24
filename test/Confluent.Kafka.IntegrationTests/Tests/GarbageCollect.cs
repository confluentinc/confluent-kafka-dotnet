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
using System.Text;
using System.Threading;
using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Make a Producer and Consumer.
        ///     Do some stuff with them.
        ///     Force a garbage collection.
        ///     Segfault?
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void GarbageCollect(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start GarbageCollect");

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            var consumerConfig = new ConsumerConfig { GroupId = Guid.NewGuid().ToString(), BootstrapServers = bootstrapServers };

            using (var producer = new Producer<Null, string>(producerConfig))
            {
                producer.ProduceAsync(singlePartitionTopic, new Message<Null, string> { Value = "test string" }).Wait();
            }

            using (var consumer = new Consumer<Null, string>(consumerConfig))
            {
                consumer.Subscribe(singlePartitionTopic);
                consumer.Consume(TimeSpan.FromMilliseconds(1000));
                consumer.Close();
            }

            // The process running the tests has probably had many created / destroyed clients by now.
            // This is an arbitrarily chosen test to put this check in.
            Assert.Equal(0, Library.HandleCount);

            GC.Collect();
            // if an attempt is made to free an unmanaged resource a second time
            // in an object finalizer, the call to .Collect() will likely segfault.
            
            LogToFile("end   GarbageCollect");
        }
    }
}
