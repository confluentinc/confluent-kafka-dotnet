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
using System.Collections.Generic;
using System.Linq;
using System;


namespace Confluent.Kafka.UnitTests.Serialization
{
    public class StringTests
    {
        [Fact]
        public void SerializeDeserialize()
        {
            Assert.Equal("hello world", Deserializers.Utf8.Deserialize(Serializers.Utf8.Serialize("hello world", SerializationContext.Empty), false, SerializationContext.Empty));
            Assert.Equal("ឆ្មាត្រូវបានហែលទឹក", Deserializers.Utf8.Deserialize(Serializers.Utf8.Serialize("ឆ្មាត្រូវបានហែលទឹក", SerializationContext.Empty), false, SerializationContext.Empty));
            Assert.Equal("вы не банан", Deserializers.Utf8.Deserialize(Serializers.Utf8.Serialize("вы не банан", SerializationContext.Empty), false, SerializationContext.Empty));
            Assert.Null(Deserializers.Utf8.Deserialize(Serializers.Utf8.Serialize(null, SerializationContext.Empty), true, SerializationContext.Empty));

            // TODO: check some serialize / deserialize operations that are not expected to work, including some
            //       cases where Deserialize can be expected to throw an exception.
        }
    }
}
