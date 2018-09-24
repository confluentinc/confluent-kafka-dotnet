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

using Confluent.Kafka.Admin;
using Xunit;


namespace Confluent.Kafka.UnitTests
{
    public class ConfigResourceTests
    {
        [Fact]
        public void Equality()
        {
            ConfigResource a = new ConfigResource { Name = "a", Type = ResourceType.Broker };
            ConfigResource b = new ConfigResource { Name = "b", Type = ResourceType.Broker };
            ConfigResource a1 = new ConfigResource { Name = "a", Type = ResourceType.Group };
            ConfigResource a2 = new ConfigResource { Name = "a", Type = ResourceType.Broker };

            Assert.NotEqual(a, b);
            Assert.NotEqual(a, a1);
            Assert.NotEqual(b, a1);
            Assert.Equal(a, a2);
            Assert.True(a == a2);
            Assert.False(a != a2);
            Assert.True(a != b);
            Assert.False(a == b);
            Assert.True(a.Equals(a2));
            Assert.False(a.Equals(b));
        }
    }
}
