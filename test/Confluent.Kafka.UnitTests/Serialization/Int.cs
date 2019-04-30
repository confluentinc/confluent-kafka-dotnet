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
using Xunit;


namespace Confluent.Kafka.UnitTests.Serialization
{
    public class IntTests
    {
        private static readonly int[] toTest = new int[]
        {
            0, 1, -1, 42, -42, 127, 128, 129, -127, -128, -129,
            254, 255, 256, 257, -254, -255, -256, -257,
            (int)short.MinValue-1, (int)short.MinValue, (int)short.MinValue+1,
            (int)short.MaxValue-1, (int)short.MaxValue, (int)short.MaxValue+1,
            int.MaxValue-1, int.MaxValue, int.MinValue, int.MinValue + 1
        };

        [Fact]
        public void IsBigEndian()
        {
            var bytes = Serializers.Int32.Serialize(42, SerializationContext.Empty);
            Assert.Equal(4, bytes.Length);
            // most significant byte in smallest address.
            Assert.Equal(0, bytes[0]);
            Assert.Equal(42, bytes[3]);
        }

        [Fact]
        public void SerializationAgreesWithSystemNetHostToNetworkOrder()
        {
            foreach (int theInt in toTest)
            {
                int networkOrder = System.Net.IPAddress.HostToNetworkOrder(theInt);
                var bytes1 = BitConverter.GetBytes(networkOrder);

                var bytes2 = Serializers.Int32.Serialize(theInt, SerializationContext.Empty);

                Assert.Equal(bytes1.Length, bytes2.Length);

                for (int i=0; i<bytes1.Length; ++i)
                {
                    Assert.Equal(bytes1[i], bytes2[i]);
                }
            }
        }

        [Fact]
        public void CanReconstructInt()
        {
            foreach (int theInt in toTest)
            {
                var reconstructed = Deserializers.Int32.Deserialize(Serializers.Int32.Serialize(theInt, SerializationContext.Empty), false, SerializationContext.Empty);
                Assert.Equal(theInt, reconstructed);
            }
        }
    }
}
