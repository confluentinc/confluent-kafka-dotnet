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
using Confluent.Kafka.Serialization;
using System.Linq;
using System;


namespace Confluent.Kafka.UnitTests.Serialization
{
    public class StringTests
    {
        [Fact]
        public void SerializeDeserialize()
        {
            Assert.Equal("hello world", new StringDeserializer(Encoding.UTF8).Deserialize("topic", new StringSerializer(Encoding.UTF8).Serialize("topic", "hello world")));
            Assert.Equal("ឆ្មាត្រូវបានហែលទឹក", new StringDeserializer(Encoding.UTF8).Deserialize("topic", new StringSerializer(Encoding.UTF8).Serialize("topic", "ឆ្មាត្រូវបានហែលទឹក")));
            Assert.Equal("вы не банан", new StringDeserializer(Encoding.UTF8).Deserialize("topic", new StringSerializer(Encoding.UTF8).Serialize("topic", "вы не банан")));
            Assert.Null(new StringDeserializer(Encoding.UTF8).Deserialize("topic", new StringSerializer(Encoding.UTF8).Serialize("topic", null)));

            // TODO: check some serialize / deserialize operations that are not expected to work, including some
            //       cases where Deserialize can be expected to throw an exception.
        }

        [Fact]
        public void DeserializerConstructKeyViaConfig()
        {
            string testString = "hello world";
            var serialized = new StringSerializer(Encoding.UTF8).Serialize("mytopic", testString);
            var config = new Dictionary<string, object>();
            config.Add("dotnet.string.deserializer.encoding.key", "utf-8");
            var deserializer = new StringDeserializer();
            var newConfig = deserializer.Configure(config, true);
            Assert.Empty(newConfig);
            Assert.Equal(testString, deserializer.Deserialize("mytopic", serialized));
        }

        [Fact]
        public void DeserializerConstructValueViaConfig()
        {
            string testString = "hello world";
            var serialized = new StringSerializer(Encoding.UTF8).Serialize("mytopic", testString);
            var config = new Dictionary<string, object>();
            config.Add("dotnet.string.deserializer.encoding.value", "utf-8");
            var deserializer = new StringDeserializer();
            var newConfig = deserializer.Configure(config, false);
            Assert.Empty(newConfig);
            Assert.Equal(testString, deserializer.Deserialize("mytopic", serialized));
        }

        [Fact]
        public void SerializerConstructKeyViaConfig()
        {
            string testString = "hello world";
            var config = new Dictionary<string, object>();
            config.Add("dotnet.string.serializer.encoding.key", "utf-8");
            var serializer = new StringSerializer();
            var newConfig = serializer.Configure(config, true);
            Assert.Empty(newConfig);
            var serialized = serializer.Serialize("mytopic", testString);
            Assert.Equal(new StringDeserializer(Encoding.UTF8).Deserialize("mytopic", serialized), testString);
        }

        [Fact]
        public void SerializerConstructValueViaConfig()
        {
            string testString = "hello world";
            var config = new Dictionary<string, object>();
            config.Add("dotnet.string.serializer.encoding.value", "utf-8");
            var serializer = new StringSerializer();
            var newConfig = serializer.Configure(config, false);
            Assert.Empty(newConfig);
            var serialized = serializer.Serialize("mytopic", testString);
            Assert.Equal(new StringDeserializer(Encoding.UTF8).Deserialize("mytopic", serialized), testString);
        }

        [Fact]
        public void SerializeDoubleConfigKey()
        {
            var config = new Dictionary<string, object>();
            config.Add("dotnet.string.serializer.encoding.value", "utf-8");
            try
            {
                var serializer = new StringSerializer(Encoding.UTF32);
                serializer.Configure(config, false);
            }
            catch (ArgumentException)
            {
                return;
            }

            Assert.True(false, "Exception expected");
        }

        [Fact]
        public void DeserializeDoubleConfigValue()
        {
            var config = new Dictionary<string, object>();
            config.Add("dotnet.string.deserializer.encoding.value", "utf-8");
            try
            {
                var deserializer = new StringDeserializer(Encoding.UTF32);
                deserializer.Configure(config, false);
            }
            catch (ArgumentException)
            {
                return;
            }

            Assert.True(false, "Exception expected");
        }

        [Fact]
        public void DeserializeInvalidConfigValue()
        {
            var config = new Dictionary<string, object>();
            config.Add("dotnet.string.deserializer.encoding.value", "invalid-encoding");
            try
            {
                var deserializer = new StringDeserializer();
                deserializer.Configure(config, false);
            }
            catch (Exception)
            {
                return;
            }

            Assert.True(false, "Exception expected");
        }

        [Fact]
        public void SerializeInvalidConfigValue()
        {
            var config = new Dictionary<string, object>();
            config.Add("dotnet.string.serializer.encoding.value", "invalid-encoding");
            try
            {
                var serializer = new StringSerializer();
                serializer.Configure(config, false);
            }
            catch (Exception)
            {
                return;
            }

            Assert.True(false, "Exception expected");
        }

        [Fact]
        public void DeserializeNoConfigValue()
        {
            try
            {
                var deserializer = new StringDeserializer();
                deserializer.Configure(new Dictionary<string, object>(), false);
            }
            catch (Exception)
            {
                return;
            }

            Assert.True(false, "Exception expected");
        }

        [Fact]
        public void SerializeNoConfigValue()
        {
            try
            {
                var serializer = new StringSerializer();
                serializer.Configure(new Dictionary<string, object>(), false);
            }
            catch (Exception)
            {
                return;
            }

            Assert.True(false, "Exception expected");
        }

    }
}
