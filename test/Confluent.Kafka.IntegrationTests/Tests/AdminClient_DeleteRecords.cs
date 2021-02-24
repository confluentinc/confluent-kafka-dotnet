// Copyright 2021 Confluent Inc.
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
using System.Linq;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test functionality of AdminClient.DeleteRecords.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void AdminClient_DeleteRecords(string bootstrapServers)
        {
            LogToFile("start AdminClient_DeleteRecords");

            using (var topic1 = new TemporaryTopic(bootstrapServers, 1))
            using (var topic2 = new TemporaryTopic(bootstrapServers, 1))
            using (var producer = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            using (var consumer = new ConsumerBuilder<Null, string>(new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = "unimportant" }).Build())
            {
                for (int i=0; i<10; ++i)
                {
                    producer.Produce(topic1.Name, new Message<Null, string> { Value = i.ToString() });
                    producer.Produce(topic2.Name, new Message<Null, string> { Value = i.ToString() });
                }
                producer.Flush(TimeSpan.FromSeconds(10));

                var r = adminClient.DeleteRecordsAsync(new List<TopicPartitionOffset>
                    {
                        new TopicPartitionOffset(topic1.Name, 0, 4),
                        new TopicPartitionOffset(topic2.Name, 0, 0) // no modification.
                    }).Result;
                var t1r = r.Where(a => a.Topic == topic1.Name).ToList()[0];
                var t2r = r.Where(a => a.Topic == topic2.Name).ToList()[0];
                Assert.Equal(4, (int)t1r.Offset);
                Assert.Equal(0, (int)t2r.Offset);

                var wm1 = consumer.QueryWatermarkOffsets(new TopicPartition(topic1.Name, 0), TimeSpan.FromSeconds(10));
                Assert.Equal(4, (int)wm1.Low);
                var wm2 = consumer.QueryWatermarkOffsets(new TopicPartition(topic2.Name, 0), TimeSpan.FromSeconds(10));
                Assert.Equal(0, (int)wm2.Low);

                r = adminClient.DeleteRecordsAsync(new List<TopicPartitionOffset>
                    {
                        new TopicPartitionOffset(topic1.Name, 0, 3)
                    }).Result;
                Assert.Equal(4, (int)r.First().Offset);

                try
                {
                    r = adminClient.DeleteRecordsAsync(new List<TopicPartitionOffset>
                        {
                            new TopicPartitionOffset(topic1.Name, 0, 12)
                        }).Result;
                    Assert.True(false); // expecting exception.
                }
                catch (Exception e)
                {
                    var ie = e.InnerException;
                    Assert.IsType<Admin.DeleteRecordsException>(ie);
                    var dre = (Admin.DeleteRecordsException)ie;
                    Assert.Single(dre.Results);
                    Assert.Equal(ErrorCode.OffsetOutOfRange, dre.Results[0].Error.Code);
                }
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   AdminClient_DeleteRecords");
        }
    }
}
