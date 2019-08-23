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


namespace Confluent.Kafka.UnitTests
{
    public class BrokerMetadataTest
    {
        [Fact]
        public void Constuctor()
        {
            var bm = new BrokerMetadata(42, "myhost", 8080);
            Assert.Equal(42, bm.BrokerId);
            Assert.Equal("myhost", bm.Host);
            Assert.Equal(8080, bm.Port);
        }

        [Fact]
        public void ToStringTest()
        {
            var bm = new BrokerMetadata(42, "myhost", 8080);
            Assert.Contains(42.ToString(), bm.ToString());
            Assert.Contains("myhost", bm.ToString());
            Assert.Contains(8080.ToString(), bm.ToString());

            // TODO: JSON based test. Note: there is coverage of this already in the Metadata integration test.
        }
    }
}

