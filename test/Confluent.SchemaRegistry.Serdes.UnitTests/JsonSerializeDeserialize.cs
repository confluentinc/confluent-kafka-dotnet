// Copyright 2020-2023 Confluent Inc.
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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;


namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    public class JsonSerializeDeserializeTests
    {
        public string schema1 = @"
{
  ""$schema"": ""http://json-schema.org/draft-07/schema#"",
  ""title"": ""Schema1"",
  ""$id"": ""schema1.json"",
  ""type"": ""object"",
  ""properties"": {
    ""field1"": {
      ""type"": ""string""
    },
    ""field2"": {
      ""type"": ""integer""
    },
    ""field3"": {
      ""$ref"": ""schema2.json#/definitions/field""
    }
  }
}
";
        public string schema2 = @"
{
  ""$schema"": ""http://json-schema.org/draft-07/schema#"",
  ""$id"": ""schema2.json"",
  ""title"": ""Schema2"",
  ""type"": ""object"",
  ""definitions"": {
    ""field"": {
      ""type"": ""boolean""
    }
  }
}
";
        public class Schema1
        {
            public string Field1 { get; set; }
            
            public int Field2 { get; set; }
            
            public bool Field3 { get; set; }
        }

        public class UInt32Value
        {
            public int Value { get; set; }
        }

#nullable enable
        public class NonNullStringValue
        {
            public string Value { get; set; } = "";

            public NestedNonNullStringValue Nested { get; set; } = new();
        }

        public class NestedNonNullStringValue
        {
            public string Value { get; set; } = "";
        }
#nullable disable

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
        private ISchemaRegistryClient schemaRegistryClientJsonRef;
        private string testTopic;
        private Dictionary<string, int> store = new Dictionary<string, int>();

        public JsonSerializeDeserializeTests()
        {
            testTopic = "topic";
            var schemaRegistryMock = new Mock<ISchemaRegistryClient>();
            schemaRegistryMock.Setup(x => x.ConstructValueSubjectName(testTopic, It.IsAny<string>())).Returns($"{testTopic}-value");
            schemaRegistryMock.Setup(x => x.RegisterSchemaAsync("topic-value", It.IsAny<string>(), It.IsAny<bool>())).ReturnsAsync(
                (string topic, string schema, bool normalize) => store.TryGetValue(schema, out int id) ? id : store[schema] = store.Count + 1
            );
            schemaRegistryMock.Setup(x => x.GetSchemaAsync(It.IsAny<int>(), It.IsAny<string>())).ReturnsAsync(
                (int id, string format) => new Schema(store.Where(x => x.Value == id).First().Key, null, SchemaType.Protobuf)
            );
            schemaRegistryClient = schemaRegistryMock.Object;

            var schemaRegistryMockJsonRef = new Mock<ISchemaRegistryClient>();
            schemaRegistryMockJsonRef.Setup(x => x.RegisterSchemaAsync("topic-Schema2", It.IsAny<Schema>(), It.IsAny<bool>())).ReturnsAsync(
                (string topic, Schema schema, bool normalize) => store.TryGetValue(schema.SchemaString, out int id) ? id : store[schema.SchemaString] = store.Count + 1
            );
            schemaRegistryMockJsonRef.Setup(x => x.RegisterSchemaAsync("topic-Schema1", It.IsAny<Schema>(), It.IsAny<bool>())).ReturnsAsync(
                (string topic, Schema schema, bool normalize) => store.TryGetValue(schema.SchemaString, out int id) ? id : store[schema.SchemaString] = store.Count + 1
            );
            schemaRegistryMockJsonRef.Setup(x => x.GetLatestSchemaAsync("topic-Schema2"))
            .ReturnsAsync((string subject) => new RegisteredSchema("topic-Schema2", 1, store.TryGetValue(schema2, out int id) ? id : store[schema2] = store.Count + 1, schema2, SchemaType.Json, new List<SchemaReference>()));
            var refs = new List<SchemaReference> { new SchemaReference("schema2.json", "topic-Schema2", 1) };
            schemaRegistryMockJsonRef.Setup(x => x.GetLatestSchemaAsync("topic-Schema1"))
            .ReturnsAsync((string subject) => new RegisteredSchema("topic-Schema1", 1, store.TryGetValue(schema1, out int id) ? id : store[schema1] = store.Count + 1, schema1, SchemaType.Json, refs));
            schemaRegistryMockJsonRef.Setup(x => x.GetRegisteredSchemaAsync("topic-Schema2", It.IsAny<int>()))
            .ReturnsAsync((string subject, int version) =>
                new RegisteredSchema("topic-Schema2", version,
                    store.TryGetValue(schema2, out int id) ? id : store[schema2] = store.Count + 1, schema2, SchemaType.Json, new List<SchemaReference>())
            );
            schemaRegistryMockJsonRef.Setup(x => x.GetRegisteredSchemaAsync("topic-Schema1", It.IsAny<int>()))
            .ReturnsAsync((string subject, int version) => new RegisteredSchema("topic-Schema1", version, store.TryGetValue(schema1, out int id) ? id : store[schema1] = store.Count + 1, schema1, SchemaType.Json, refs)
            );
            schemaRegistryClientJsonRef = schemaRegistryMockJsonRef.Object;
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

        [Fact]
        public async Task WithJsonSchemaExternalReferencesAsync()
        {
            var subject1 = $"{testTopic}-Schema1";
            var subject2 = $"{testTopic}-Schema2";

            var id2 = schemaRegistryClientJsonRef.RegisterSchemaAsync(subject2, new Schema(schema2, Confluent.SchemaRegistry.SchemaType.Json)).Result;
            var s2 = schemaRegistryClientJsonRef.GetLatestSchemaAsync(subject2).Result;
            var refs = new List<SchemaReference> { new SchemaReference("schema2.json", subject2, s2.Version) };
            var id1 = schemaRegistryClientJsonRef.RegisterSchemaAsync(subject1, new Schema(schema1, refs, Confluent.SchemaRegistry.SchemaType.Json)).Result;
            var s1 = schemaRegistryClientJsonRef.GetLatestSchemaAsync(subject1).Result;
            var unreg_schema1 = s1.Schema;
            var jsonSerializerConfig = new JsonSerializerConfig
            {
                UseLatestVersion = true,
                AutoRegisterSchemas = false,
                SubjectNameStrategy = SubjectNameStrategy.TopicRecord
            };
            
            var jsonSchemaGeneratorSettings = new JsonSchemaGeneratorSettings
            {
                SerializerSettings = new JsonSerializerSettings
                {
                    ContractResolver = new DefaultContractResolver
                    {
                        NamingStrategy = new CamelCaseNamingStrategy()
                    }
                }
            };
            
            var jsonSerializer = new JsonSerializer<Schema1>(schemaRegistryClientJsonRef, unreg_schema1,
                jsonSerializerConfig, jsonSchemaGeneratorSettings);
            var jsonDeserializer = new JsonDeserializer<Schema1>(schemaRegistryClientJsonRef, unreg_schema1);
            var v = new Schema1
            {
                Field1 = "Hello",
                Field2 = 123,
                Field3 = true
            };
            string expectedJson = "{\"field1\":\"Hello\",\"field2\":123,\"field3\":true}";
            var bytes = await jsonSerializer.SerializeAsync(v, new SerializationContext(MessageComponentType.Value, testTopic));
            Assert.NotNull(bytes);
            Assert.Equal(expectedJson, Encoding.UTF8.GetString(bytes.AsSpan().Slice(5)));

            var actual = await jsonDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic));
            Assert.Equal(v.Field3, actual.Field3);
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

        [Fact]
        public async Task ValidationFailureReturnsPath()
        {
            var jsonSerializer = new JsonSerializer<NonNullStringValue>(schemaRegistryClient);

            var v = new NonNullStringValue { Value = null };

            try
            {
                await jsonSerializer.SerializeAsync(v, new SerializationContext(MessageComponentType.Value, testTopic));
                Assert.True(false, "Serialization did not throw an expected exception");
            }
            catch (InvalidDataException ex)
            {
                Assert.Equal("Schema validation failed for properties: [#/Value]", ex.Message);
            }
            catch (Exception ex)
            {
                Assert.True(false, $"Serialization threw exception of type {ex.GetType().FullName} instead of the expected {typeof(InvalidDataException).FullName}");
            }
        }

        [Fact]
        public async Task NestedValidationFailureReturnsPath()
        {
            var jsonSerializer = new JsonSerializer<NonNullStringValue>(schemaRegistryClient);

            var v = new NonNullStringValue
            {
                Nested = new()
                {
                    Value = null
                }
            };

            try
            {
                await jsonSerializer.SerializeAsync(v, new SerializationContext(MessageComponentType.Value, testTopic));
                Assert.True(false, "Serialization did not throw an expected exception");
            }
            catch (InvalidDataException ex)
            {
                Assert.Equal("Schema validation failed for properties: [#/Nested.Value]", ex.Message);
            }
            catch (Exception ex)
            {
                Assert.True(false, $"Serialization threw exception of type {ex.GetType().FullName} instead of the expected {typeof(InvalidDataException).FullName}");
            }
        }
    }
}
