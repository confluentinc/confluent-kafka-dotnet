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

using System;
using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.Tests
{
    public class InvalidHandleTest
    {
        /// <summary>
        ///     Test that the Consumer/Producer constructors throws an
        ///     exception if the creation of kafka handle fails and that
        ///     the correct exception is thrown.
        /// </summary>
        [Fact]
        public void KafkaHandleCreation()
        {
            var config = new Dictionary<string, object>
            {
                ["group.id"] = "test",
                ["sasl.mechanisms"] = "PLAIN",
                ["security.protocol"] = "ssl",
                ["ssl.ca.location"] = "invalid"
            };
            
            InvalidOperationException e = Assert.Throws<InvalidOperationException>(() => new Consumer(config));
            Assert.Contains("ssl.ca.location failed", e.Message);
            // note: if this test fail, it may be because an other error is thrown
            // in a new librdkafka version, adpat test in this case

            e = Assert.Throws<InvalidOperationException>(() => new Consumer<Null, Null>(config, null, null));
            Assert.Contains("ssl.ca.location failed", e.Message);

            e = Assert.Throws<InvalidOperationException>(() => new Producer(config));
            Assert.Contains("ssl.ca.location failed", e.Message);

            e = Assert.Throws<InvalidOperationException>(() => new Producer<Null, Null>(config, null, null));
            Assert.Contains("ssl.ca.location failed", e.Message);
        }
    }
}
