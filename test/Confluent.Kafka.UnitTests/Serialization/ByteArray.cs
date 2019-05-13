// Copyright 2016-2019 Confluent Inc.
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


namespace Confluent.Kafka.UnitTests.Serialization
{
    public class ByteArrayTests
    {
        [Theory]
        [InlineData(new byte[0])]
        [InlineData(new byte[] { 1 })]
        [InlineData(new byte[] { 1, 2, 3, 4, 5 })]
        public void CanReconstructByteArray(byte[] values)
        {
            Assert.Equal(values, Deserializers.ByteArray.Deserialize(Serializers.ByteArray.Serialize(values, SerializationContext.Empty), false, SerializationContext.Empty));
        }

        [Fact]
        public void CanReconstructByteArrayNull()
        {
            Assert.Null(Deserializers.ByteArray.Deserialize(Serializers.ByteArray.Serialize(null, SerializationContext.Empty), true, SerializationContext.Empty));
        }
    }
}
