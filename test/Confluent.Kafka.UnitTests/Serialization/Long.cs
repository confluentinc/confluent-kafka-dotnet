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


namespace Confluent.Kafka.Serialization.Tests
{
    public class LongTests
    {
        [Fact]
        public void SerializeDeserialize()
        {
            Assert.Equal(0, new LongDeserializer().Deserialize(new LongSerializer().Serialize(0)));
            Assert.Equal(100L, new LongDeserializer().Deserialize(new LongSerializer().Serialize(100L)));
            Assert.Equal(9223372036854775807, new LongDeserializer().Deserialize(new LongSerializer().Serialize(9223372036854775807)));
            Assert.Equal(-9223372036854775807, new LongDeserializer().Deserialize(new LongSerializer().Serialize(-9223372036854775807)));
        }
    }
}
