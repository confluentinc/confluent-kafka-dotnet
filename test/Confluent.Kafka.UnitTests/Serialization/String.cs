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
using System.Text;


namespace Confluent.Kafka.Serialization.Tests
{
    public class StringTests
    {
        [Fact]
        public void SerializeDeserialize()
        {
            Assert.Equal("hello world", new StringDeserializer(Encoding.UTF8).Deserialize(new StringSerializer(Encoding.UTF8).Serialize("hello world")));
            Assert.Equal("ឆ្មាត្រូវបានហែលទឹក", new StringDeserializer(Encoding.UTF8).Deserialize(new StringSerializer(Encoding.UTF8).Serialize("ឆ្មាត្រូវបានហែលទឹក")));
            Assert.Equal("вы не банан", new StringDeserializer(Encoding.UTF8).Deserialize(new StringSerializer(Encoding.UTF8).Serialize("вы не банан")));
            Assert.Equal(null, new StringDeserializer(Encoding.UTF8).Deserialize(new StringSerializer(Encoding.UTF8).Serialize(null)));

            // TODO: check some serialize / deserialize operations that are not expected to work, including some
            //       cases where Deserialize can be expected to throw an exception.
        }


        [Fact]
        public void ExplicitSerializationWork()
        {
            foreach (string topic in new[] { null, "", "topic" })
            {
                var serializer = new StringSerializer(Encoding.UTF8);
                var bytesImplicit = serializer.Serialize("42");
                var bytesExplicit = ((ISerializer<string>)serializer).Serialize(topic, "42");

                Assert.Equal(bytesImplicit, bytesExplicit);
            }
        }

        [Fact]
        public void ExplicitDeserializationWorks()
        {
            var byte42 = new StringSerializer(Encoding.UTF8).Serialize("42");

            foreach (string topic in new[] { null, "", "topic" })
            {
                var deserializer = new StringDeserializer(Encoding.UTF8);
                var deconstructExplicit = ((IDeserializer<string>)deserializer).Deserialize(topic, byte42);

                Assert.Equal("42", deconstructExplicit);
            }
        }
    }
}
