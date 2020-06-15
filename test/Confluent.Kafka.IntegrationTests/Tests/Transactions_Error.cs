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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test a code path that marshals an `error_t` object.
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Transactions_Error(string bootstrapServers)
        {
            LogToFile("start Transactions_Error");

            var defaultTimeout = TimeSpan.FromSeconds(30);

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            {
                using (var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrapServers, TransactionalId = Guid.NewGuid().ToString() }).Build())
                {
                    producer.InitTransactions(defaultTimeout);
                    producer.BeginTransaction();
                    Assert.Throws<KafkaException>(() => { producer.BeginTransaction(); });
                }
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Transactions_Error");
        }
    }
}
