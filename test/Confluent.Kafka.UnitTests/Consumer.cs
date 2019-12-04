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

using Xunit;
using System;
using System.Text;
using System.Collections.Generic;


namespace Confluent.Kafka.UnitTests
{
    public class ConsumerTests
    {
        [Fact]
        public void Constructor()
        {
            // Throw exception if 'group.id' is not set in config and ensure that exception
            // mentions 'group.id'.
            var config = new ConsumerConfig();
            var e = Assert.Throws<ArgumentException>(() => { var c = new ConsumerBuilder<byte[], byte[]>(config).Build(); });
            Assert.Contains("group.id", e.Message);

            // Throw exception if a config value is null and ensure that exception mentions the
            // respective config key.
            var configWithNullValue = CreateValidConfiguration();
            configWithNullValue.Set("sasl.password", null);
            e = Assert.Throws<ArgumentNullException>(() => { var c = new ConsumerBuilder<byte[], byte[]>(configWithNullValue).Build(); });
            Assert.Contains("sasl.password", e.Message);

            // Throw an exception if dotnet.cancellation.delay.max.ms is out of range.
            e = Assert.Throws<ArgumentOutOfRangeException>(() =>
            {
                var c = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
                {
                    BootstrapServers = "localhost:9092",
                    GroupId = Guid.NewGuid().ToString(),
                    CancellationDelayMaxMs = 0
                }).Build();
            });
            Assert.Contains("range", e.Message);
            e = Assert.Throws<ArgumentOutOfRangeException>(() =>
            {
                var c = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
                {
                    BootstrapServers = "localhost:9092",
                    GroupId = Guid.NewGuid().ToString(),
                    CancellationDelayMaxMs = 10001
                }).Build();
            });
            Assert.Contains("range", e.Message);
        }

        [Fact]
        public void Constructor_ConsumerTxn()
        {
            // should not throw
            using (var c = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
                {
                    BootstrapServers = "localhost:666",
                    GroupId = Guid.NewGuid().ToString(),
                    IsolationLevel = IsolationLevel.ReadCommitted
                }).Build())
            { }

            // should not throw
            using (var c = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
                {
                    BootstrapServers = "localhost:666",
                    GroupId = Guid.NewGuid().ToString(),
                    IsolationLevel = IsolationLevel.ReadUncommitted
                }).Build())
            { }
        }

        private static ConsumerConfig CreateValidConfiguration()
        {
            return new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "my-group"
            };
        }
    }
}
