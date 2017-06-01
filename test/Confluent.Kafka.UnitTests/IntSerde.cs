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
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka.Tests
{
    public class IntSerdeTests
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
            var serializer = new IntSerializer();
            var bytes = serializer.Serialize(42);
            Assert.Equal(bytes.Length, 4);
            // most significant byte in smallest address.
            Assert.Equal(bytes[0], 0);
            Assert.Equal(bytes[3], 42);
        }

        [Fact]
        public void SerializationAgreesWithSystemNetHostToNetworkOrder()
        {
            foreach (int theInt in toTest)
            {
                int networkOrder = System.Net.IPAddress.HostToNetworkOrder(theInt);
                var bytes1 = BitConverter.GetBytes(networkOrder);

                var serializer = new IntSerializer();
                var bytes2 = serializer.Serialize(theInt);

                Assert.Equal(bytes1.Length, bytes2.Length);

                for (int i=0; i<bytes1.Length; ++i)
                {
                    Assert.Equal(bytes1[i], bytes2[i]);
                }
            }
        }

        [Fact]
        public void CanReconstruct()
        {
            var serializer = new IntSerializer();
            var deserializer = new IntDeserializer();

            foreach (int theInt in toTest)
            {
                var reconstructed = deserializer.Deserialize(serializer.Serialize(theInt));
                Assert.Equal(theInt, reconstructed);
            }
        }

        [Fact]
        public void ExplicitSerializationWorks()
        {
            foreach(string topic in new[] {null, "", "topic"})
            {
                var serializer = new IntSerializer();
                var bytesImplicit = serializer.Serialize(42);
                var bytesExplicit = ((ISerializer<int>)serializer).Serialize(topic, 42);

                Assert.Equal(bytesImplicit, bytesExplicit);
            }
        }

        [Fact]
        public void ExplicitDeserializationWorks()
        {
            var byte42 = new IntSerializer().Serialize(42);

            foreach (string topic in new[] { null, "", "topic" })
            {
                var deserializer = new IntDeserializer();
                var deconstructExplicit = ((IDeserializer<int>)deserializer).Deserialize(topic, byte42);

                Assert.Equal(42, deconstructExplicit);
            }
        }
    }
}
