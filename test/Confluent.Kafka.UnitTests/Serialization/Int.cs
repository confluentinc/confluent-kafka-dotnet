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

using Confluent.Kafka.Serialization;
using Xunit;

namespace Confluent.Kafka.UnitTests
{
    public class IntTests
    {
        [Theory]
        [InlineData(int.MaxValue)]
        [InlineData(int.MinValue)]
        [InlineData(-1)]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(1234567890)]
        public void SerializeDeserialize(int value)
        {
            Assert.Equal(value, new IntDeserializer().Deserialize(new IntSerializer().Serialize(value)));
        }

        [Theory]
        [InlineData(new byte[] { 0x12, 0x34, 0x56, 0x78 }, 0x12345678)]
        public void DeserializeOrder(byte[] data, int value)
        {
            var deserializedValue = new IntDeserializer().Deserialize(data);
            Assert.Equal(value, deserializedValue);
        }
    }
}
