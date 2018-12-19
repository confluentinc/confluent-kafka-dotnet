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
            Assert.Equal("hello world", Deserializers.UTF8(Serializers.UTF8("hello world"), false));
            Assert.Equal("ឆ្មាត្រូវបានហែលទឹក", Deserializers.UTF8(Serializers.UTF8("ឆ្មាត្រូវបានហែលទឹក"), false));
            Assert.Equal("вы не банан", Deserializers.UTF8(Serializers.UTF8("вы не банан"), false));
            Assert.Null(Deserializers.UTF8(Serializers.UTF8(null), true));

            // TODO: check some serialize / deserialize operations that are not expected to work, including some
            //       cases where Deserialize can be expected to throw an exception.
        }
    }
}
