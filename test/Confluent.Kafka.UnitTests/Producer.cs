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
    public class ProducerTests
    {
        [Fact]
        public void Constuctor_Validate_Null_Config_Throws()
        {
            // Throw exception if a config value is null and ensure that exception mentions the
            // respective config key.
            var configWithNullValue = new ProducerConfig();
            configWithNullValue.Set("sasl.password", null);
            var e = Assert.Throws<ArgumentException>(() => { var p = new Producer<byte[], byte[]>(configWithNullValue); });
            Assert.Contains("sasl.password", e.Message);
        }

        [Fact]
        public void Constuctor_Ensure_Different_Serializers()
        {
            // Throw exception when serializer and deserializer are equal and ensure that exception
            // message indicates the issue.
            var e = Assert.Throws<ArgumentException>(() =>
            {
                var validConfig = CreateValidConfiguration();
                var deserializer = Serializers.UTF8;
                var p = new Producer<string, string>(validConfig, deserializer, deserializer);
            });
            Assert.Contains("must not be the same object", e.Message);
        }

        [Fact]
        public void Constuctor_Ensure_Default_Serializers()
        {
            var validConfig = CreateValidConfiguration();

            // try creating some typical combinations
            new Producer<string, string>(validConfig);
            new Producer<byte[], byte[]>(validConfig);
            new Producer<byte[], string>(validConfig);
            new Producer<Null, string>(validConfig);
            new Producer<int, string>(validConfig);
            new Producer<long, string>(validConfig);
            new Producer<float, string>(validConfig);
            new Producer<double, string>(validConfig);
        }

        [Fact]
        public void Constuctor_Ignore_Not_Valid()
        {
            // Throw exception when type is Ignore
            var e = Assert.Throws<ArgumentException>(() =>
            {
                var validConfig = CreateValidConfiguration();
                var p = new Producer<Ignore, string>(validConfig);
            });
            Assert.Contains("Serializer not valid for Ignore", e.Message);

            e = Assert.Throws<ArgumentException>(() =>
            {
                var validConfig = CreateValidConfiguration();
                var p = new Producer<string, Ignore>(validConfig);
            });
            Assert.Contains("Serializer not valid for Ignore", e.Message);
        }

        private static ProducerConfig CreateValidConfiguration()
        {
            return new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };
        }
    }
}
