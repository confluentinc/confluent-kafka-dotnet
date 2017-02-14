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

using System.Linq;
using System.Text;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka.Serialization;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Basic DeserializingConsumer test (background poll mode).
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void DeserializingConsumer_Background(string bootstrapServers, string topic)
        {
            int N = 2;
            var firstProduced = Util.ProduceMessages(bootstrapServers, topic, 100, N);

            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", "simple-produce-consume" },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 }
            };

            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                int msgCnt = 0;
                AutoResetEvent are = new AutoResetEvent(false);

                consumer.OnMessage += (_, msg) =>
                {
                    Assert.Equal(msg.Error.Code, ErrorCode.NoError);
                    msgCnt += 1;
                };

                consumer.OnPartitionEOF += (_, partition)
                    => are.Set();

                consumer.OnPartitionsAssigned += (_, partitions) =>
                {
                    Assert.Equal(partitions.Count, 1);
                    Assert.Equal(partitions[0], firstProduced.TopicPartition);
                    consumer.Assign(partitions.Select(p => new TopicPartitionOffset(p, firstProduced.Offset)));
                };

                consumer.OnPartitionsRevoked += (_, partitions)
                    => consumer.Unassign();

                consumer.Subscribe(topic);

                consumer.Start();
                are.WaitOne();
                consumer.Stop();

                Assert.Equal(msgCnt, N);
            }
        }

    }
}
