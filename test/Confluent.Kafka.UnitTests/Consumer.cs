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
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka.Tests
{
    public class ConsumerTests
    {
        /// <summary>
        ///     Test that the Consumer constructor throws an exception if
        ///     the group.id configuration parameter is not set and that
        ///     the message of the exception mentions group.id (i.e. is
        ///     not some unrelated exception).
        /// </summary>
        [Fact]
        public void Constuctor()
        {
            var config = new Dictionary<string, object>();
            var e = Assert.Throws<ArgumentException>(() => { var c = new Consumer(config); });
            Assert.True(e.Message.Contains("group.id"));
            e = Assert.Throws<ArgumentException>(() => { var c = new Consumer<Null, string>(config, null, new StringDeserializer(Encoding.UTF8)); });
            Assert.True(e.Message.Contains("group.id"));

            // positve case covered by integration tests. here, avoiding creating a rd_kafka_t instance.
        }
    }
}
