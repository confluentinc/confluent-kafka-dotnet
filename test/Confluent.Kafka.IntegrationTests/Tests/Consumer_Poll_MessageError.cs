// Copyright 2022 Confluent Inc.
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
using System.Linq;
using System.Text;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test that an error that originates from within librdkafka during
        ///     a call to Consume surfaces via a ConsumeException.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_Poll_MessageError(string bootstrapServers)
        {
            LogToFile("start Consumer_Poll_MessageError");

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
            };

            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                var nonExistantTopic = Guid.NewGuid().ToString();
                ErrorCode code = ErrorCode.NoError;
                try
                {
                    consumer.Subscribe(nonExistantTopic);
                    consumer.Consume(TimeSpan.FromSeconds(10));
                }
                catch (ConsumeException e)
                {
                    code = e.Error.Code;
                    Assert.NotNull(e.Error.Reason);
                }
                Assert.Equal(ErrorCode.UnknownTopicOrPart, code);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_Poll_MessageError");
        }

    }
}
