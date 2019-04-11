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

namespace Confluent.Kafka.UnitTests.Serialization
{
    public class LongTests
    {
        [Theory]
        [MemberData(nameof(TestData))]
        public void CanReconstructLong(long value)
        {
            Assert.Equal(value, Deserializers.Int64.Deserialize(Serializers.Int64.Serialize(value, SerializationContext.Empty), false, SerializationContext.Empty));
        }

        [Fact]
        public void IsBigEndian()
        {
            var data = Serializers.Int64.Serialize(23L, SerializationContext.Empty);
            Assert.Equal(23, data[7]);
            Assert.Equal(0, data[0]);
        }

        [Fact]
        public void DeserializeArgNull()
        {
            Assert.ThrowsAny<ArgumentNullException>(() => Deserializers.Int64.Deserialize(null, true, SerializationContext.Empty));
        }

        [Fact]
        public void DeserializeArgLengthNotEqual8Throw()
        {
            Assert.ThrowsAny<ArgumentException>(() => Deserializers.Int64.Deserialize(new byte[7], false, SerializationContext.Empty));
            Assert.ThrowsAny<ArgumentException>(() => Deserializers.Int64.Deserialize(new byte[9], false, SerializationContext.Empty));
        }

        public static IEnumerable<object[]> TestData()
        {
            long[] testData = new long[]
            {
                0, 1, -1, 42, -42, 127, 128, 129, -127, -128,
                -129,254, 255, 256, 257, -254, -255, -256, -257,
                (int)short.MinValue-1, (int)short.MinValue, (int)short.MinValue+1,
                (int)short.MaxValue-1, (int)short.MaxValue, (int)short.MaxValue+1,
                int.MaxValue-1, int.MaxValue, int.MinValue, int.MinValue + 1,
                long.MaxValue-1,long.MaxValue,long.MinValue,long.MinValue+1
            };

            foreach (var v in testData)
            {
                yield return new object[] { v };
            }
        }
    }
}
