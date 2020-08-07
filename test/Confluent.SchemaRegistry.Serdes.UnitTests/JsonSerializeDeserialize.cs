// Copyright 2020 Confluent Inc.
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

// ConstructValueSubjectName is still used a an internal implementation detail.
#pragma warning disable CS0618

using Confluent.Kafka;
using Moq;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using NJsonSchema.Generation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;


namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    public class JsonSerializeDeserialzeTests
    {
        public class UInt32Value
        {
            public int Value { get; set; }
        }

        private class UInt32ValueMultiplyConverter : JsonConverter
        {
            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                var newValue = ((UInt32Value) value).Value * 2;
                writer.WriteStartObject();
                writer.WritePropertyName("Value");
                writer.WriteValue(newValue);
                writer.WriteEndObject();
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                if (reader.TokenType == JsonToken.StartObject)
                {
                    reader.Read();
                }

                var value = reader.ReadAsInt32() ?? 0;
                reader.Read();
                return new UInt32Value
                {
                    Value = value / 2
                };
            }

            public override bool CanConvert(Type objectType) => objectType == typeof(UInt32Value);
        }

        public enum EnumType
        {
            None,
            EnumValue = 1234,
            OtherValue = 5678
        }

        public class EnumObject
        {
            public EnumType Value { get; set; }
        }

        private ISchemaRegistryClient schemaRegistryClient;
        private string testTopic;
        private Dictionary<string, int> store = new Dictionary<string, int>();

        public JsonSerializeDeserialzeTests()
        {
            testTopic = "topic";
            var schemaRegistryMock = new Mock<ISchemaRegistryClient>();
            schemaRegistryMock.Setup(x => x.ConstructValueSubjectName(testTopic, It.IsAny<string>())).Returns($"{testTopic}-value");
            schemaRegistryMock.Setup(x => x.RegisterSchemaAsync("topic-value", It.IsAny<string>())).ReturnsAsync(
                (string topic, string schema) => store.TryGetValue(schema, out int id) ? id : store[schema] = store.Count + 1
            );
            schemaRegistryMock.Setup(x => x.GetSchemaAsync(It.IsAny<int>(), It.IsAny<string>())).ReturnsAsync(
                (int id, string format) => new Schema(store.Where(x => x.Value == id).First().Key, null, SchemaType.Protobuf)
            );
            schemaRegistryClient = schemaRegistryMock.Object;   
        }

        [Fact]
        public void Null()
        {
            var jsonSerializer = new JsonSerializer<UInt32Value>(schemaRegistryClient);
            var jsonDeserializer = new JsonDeserializer<UInt32Value>();

            var bytes = jsonSerializer.SerializeAsync(null, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Null(bytes);
            Assert.Null(jsonDeserializer.DeserializeAsync(bytes, true, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }


        [Fact]
        public void UInt32SerDe()
        {
            var jsonSerializer = new JsonSerializer<UInt32Value>(schemaRegistryClient);
            var jsonDeserializer = new JsonDeserializer<UInt32Value>();

            var v = new UInt32Value { Value = 1234 };
            var bytes = jsonSerializer.SerializeAsync(v, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal(v.Value, jsonDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result.Value);
        }

        [Fact]
        public async Task WithJsonSerializerSettingsSerDe()
        {
            const int value = 1234;
            var expectedJson = $"{{\"Value\":{value * 2}}}";
            var jsonSchemaGeneratorSettings = new JsonSchemaGeneratorSettings
            {
                SerializerSettings = new JsonSerializerSettings
                {
                    Converters = new List<JsonConverter>
                    {
                        new UInt32ValueMultiplyConverter()
                    },
                    ContractResolver = new DefaultContractResolver()
                }
            };

            var jsonSerializer = new JsonSerializer<UInt32Value>(schemaRegistryClient, jsonSchemaGeneratorSettings: jsonSchemaGeneratorSettings);
            var jsonDeserializer = new JsonDeserializer<UInt32Value>(jsonSchemaGeneratorSettings: jsonSchemaGeneratorSettings);

            var v = new UInt32Value { Value = value };
            var bytes = await jsonSerializer.SerializeAsync(v, new SerializationContext(MessageComponentType.Value, testTopic));
            Assert.NotNull(bytes);
            Assert.Equal(expectedJson, Encoding.UTF8.GetString(bytes.AsSpan().Slice(5)));

            var actual = await jsonDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic));
            Assert.NotNull(actual);
            Assert.Equal(v.Value, actual.Value);
        }

        [Theory]
        [InlineData(EnumHandling.CamelCaseString, EnumType.EnumValue, "{\"Value\":\"enumValue\"}")]
        [InlineData(EnumHandling.String, EnumType.None, "{\"Value\":\"None\"}")]
        [InlineData(EnumHandling.Integer, EnumType.OtherValue, "{\"Value\":5678}")]
        public async Task WithJsonSchemaGeneratorSettingsSerDe(EnumHandling enumHandling, EnumType value, string expectedJson)
        {
            var jsonSchemaGeneratorSettings = new JsonSchemaGeneratorSettings
            {
                DefaultEnumHandling = enumHandling
            };

            var jsonSerializer = new JsonSerializer<EnumObject>(schemaRegistryClient, jsonSchemaGeneratorSettings: jsonSchemaGeneratorSettings);
            var jsonDeserializer = new JsonDeserializer<EnumObject>(jsonSchemaGeneratorSettings: jsonSchemaGeneratorSettings);

            var v = new EnumObject { Value = value };
            var bytes = await jsonSerializer.SerializeAsync(v, new SerializationContext(MessageComponentType.Value, testTopic));
            Assert.NotNull(bytes);
            Assert.Equal(expectedJson, Encoding.UTF8.GetString(bytes.AsSpan().Slice(5)));

            var actual = await jsonDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic));
            Assert.NotNull(actual);
            Assert.Equal(actual.Value, value);
        }
    }
}
