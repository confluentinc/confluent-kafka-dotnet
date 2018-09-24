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
        public void Constuctor()
        {
            // Throw exception if 'group.id' is not set in config and ensure that exception
            // mentions 'group.id'.
            var config = new ConsumerConfig();
            var e = Assert.Throws<ArgumentException>(() => { var c = new Consumer<byte[], byte[]>(config); });
            Assert.Contains("group.id", e.Message);
            e = Assert.Throws<ArgumentException>(() => { var c = new Consumer<Null, string>(config); });
            Assert.Contains("group.id", e.Message);

            // Throw exception if a config value is null and ensure that exception mentions the
            // respective config key.
            var configWithNullValue = CreateValidConfiguration();
            configWithNullValue.Set("sasl.password", null);
            e = Assert.Throws<ArgumentException>(() => { var c = new Consumer<byte[], byte[]>(configWithNullValue); });
            Assert.Contains("sasl.password", e.Message);

            // Throw exception when serializer and deserializer are equal and ensure that exception
            // message indicates the issue.
            e = Assert.Throws<ArgumentException>(() => 
            {
                var validConfig = CreateValidConfiguration();
                var deserializer = Deserializers.UTF8;
                var c = new Consumer<string, string>(validConfig, deserializer, deserializer); 
            });
            Assert.Contains("must not be the same object", e.Message);

            // positve case covered by integration tests. here, avoiding creating a rd_kafka_t instance.
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
