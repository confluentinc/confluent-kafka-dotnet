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
    public class DoubleTests
    {
        [Fact]
        public void CanReconstructDouble()
        {
            foreach (var value in TestData)
            {
                Assert.Equal(value, Deserializers.Double.Deserialize(Serializers.Double.Serialize(value, SerializationContext.Empty), false, SerializationContext.Empty));
            }
        }

        [Fact]
        public void IsBigEndian()
        {
            var buffer = new byte[] { 23, 0, 0, 0, 0, 0, 0, 0 };
            var value = BitConverter.ToDouble(buffer, 0);
            var data = Serializers.Double.Serialize(value, SerializationContext.Empty);
            Assert.Equal(23, data[7]);
            Assert.Equal(0, data[0]);
        }

        [Fact]
        public void DeserializeArgNullThrow()
        {
            Assert.ThrowsAny<ArgumentNullException>(() => Deserializers.Double.Deserialize(null, true, SerializationContext.Empty));
        }

        [Fact]
        public void DeserializeArgLengthNotEqual8Throw()
        {
            Assert.ThrowsAny<ArgumentException>(() => Deserializers.Double.Deserialize(new byte[0], false, SerializationContext.Empty));
            Assert.ThrowsAny<ArgumentException>(() => Deserializers.Double.Deserialize(new byte[7], false, SerializationContext.Empty));
            Assert.ThrowsAny<ArgumentException>(() => Deserializers.Double.Deserialize(new byte[9], false, SerializationContext.Empty));
        }

        public static double[] TestData
        {
            get
            {
                double[] testData = new double[]
                {
                    0, 1, -1, 42, -42, 127, 128, 129, -127, -128,
                    -129,254, 255, 256, 257, -254, -255, -256, -257,
                    short.MinValue-1, short.MinValue, short.MinValue+1,
                    short.MaxValue-1, short.MaxValue,short.MaxValue+1,
                    int.MaxValue-1, int.MaxValue, int.MinValue, int.MinValue + 1,
                    double.MaxValue-1,double.MaxValue,double.MinValue,double.MinValue+1,
                    double.NaN,double.PositiveInfinity,double.NegativeInfinity,double.Epsilon,-double.Epsilon
                };

                return testData;
            }
        }
    }
}
