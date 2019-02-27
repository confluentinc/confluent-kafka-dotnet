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
using System.Threading;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Tests that ObjectDisposedException is thrown rather than AccessViolationException
        ///     when Dispose has been called
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_ClosedHandle(string bootstrapServers)
        {
            LogToFile("start Producer_ClosedHandle");

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                EnableBackgroundPoll = false
            };
            var producer = new ProducerBuilder<Null, Null>(producerConfig).Build();
            producer.Poll(TimeSpan.FromMilliseconds(10));
            producer.Dispose();
            Assert.Throws<ObjectDisposedException>(() => producer.Poll(TimeSpan.FromMilliseconds(10)));

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_ClosedHandle");
        }

        /// <summary>
        ///     Tests that ObjectDisposedException is thrown rather than AccessViolationException
        ///     when Dispose has been called
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_ClosedHandle(string bootstrapServers)
        {
            LogToFile("start Consumer_ClosedHandle");

            var consumerConfig = new ConsumerConfig { GroupId = Guid.NewGuid().ToString(), BootstrapServers = bootstrapServers };
            var consumer = new ConsumerBuilder<Null, Null>(consumerConfig).Build();
            consumer.Consume(TimeSpan.FromSeconds(10));
            consumer.Dispose();
            Assert.Throws<ObjectDisposedException>(() => consumer.Consume(TimeSpan.FromSeconds(10)));
            
            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_ClosedHandle");
        }

        /// <summary>
        ///     Tests that ObjectDisposedException is thrown rather than AccessViolationException
        ///     when Dispose has been called
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void TypedProducer_ClosedHandle(string bootstrapServers)
        {
            LogToFile("start TypedProducer_ClosedHandle");

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            var producer = new ProducerBuilder<Null, Null>(producerConfig).Build();
            producer.Flush(TimeSpan.FromMilliseconds(10));
            producer.Dispose();
            Thread.Sleep(TimeSpan.FromMilliseconds(500)); // kafka handle destroy is done on the poll thread, is not immediate.
            Assert.Throws<ObjectDisposedException>(() => producer.Flush(TimeSpan.FromMilliseconds(10)));

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   TypedProducer_ClosedHandle");
        }

        /// <summary>
        ///     Tests that ObjectDisposedException is thrown rather than AccessViolationException
        ///     when Dispose has been called
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void TypedConsumer_ClosedHandle(string bootstrapServers)
        {
            LogToFile("start TypedConsumer_ClosedHandle");

            var consumerConfig = new ConsumerConfig { GroupId = Guid.NewGuid().ToString(), BootstrapServers = bootstrapServers };
            var consumer = new ConsumerBuilder<Null, Null>(consumerConfig).Build();
            consumer.Consume(TimeSpan.FromSeconds(10));
            consumer.Dispose();
            Assert.Throws<ObjectDisposedException>(() => consumer.Consume(TimeSpan.FromSeconds(10)));

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   TypedConsumer_ClosedHandle");
        }
    }
}
