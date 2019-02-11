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
        public void Constuctor()
        {
            // Throw exception if a config value is null and ensure that exception mentions the
            // respective config key.
            var configWithNullValue = new ProducerConfig();
            configWithNullValue.Set("sasl.password", null);
            var e = Assert.Throws<ArgumentNullException>(() => { var c = new ProducerBuilder<byte[], byte[]>(configWithNullValue).Build(); });
            Assert.Contains("sasl.password", e.Message);
        }
    }
}
