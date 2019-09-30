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

using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.UnitTests
{
    public class MetadataTests
    {
        [Fact]
        public void Constuctor()
        {
            var brokers = new List<BrokerMetadata>();
            var topics = new List<TopicMetadata>();
            var md = new Metadata(brokers, topics, 42, "broker1");
            Assert.Same(brokers, md.Brokers);
            Assert.Same(topics, md.Topics);
            Assert.Equal(42, md.OriginatingBrokerId);
            Assert.Equal("broker1", md.OriginatingBrokerName);
        }

        // TODO: ToString() tests. Note: there is coverage of this already in the Metadata integration test.
    }
}
