// Copyright 2026 Confluent Inc.
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

using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     End-to-end tests that a <c>confluent.type.Variant</c> field carrying the
    ///     <c>variant</c> logical type round-trips through the real <see cref="AvroSerializer{T}" />
    ///     and <see cref="AvroDeserializer{T}" /> as a first-class <see cref="Variant" /> — i.e. the
    ///     logical type is auto-registered by the serdes constructors (no explicit
    ///     <c>LogicalTypeFactory</c> setup by the caller).
    /// </summary>
    public class VariantAvroSerdeTests : BaseSerializeDeserializeTests
    {
        // Outer record with a variant-typed field. The nested record is named
        // confluent.type.Variant and carries logicalType:variant, so the registered
        // VariantLogicalType converts it to/from a Variant at the field.
        private const string HolderSchema = @"
{
  ""type"": ""record"",
  ""name"": ""VariantHolder"",
  ""fields"": [
    { ""name"": ""id"", ""type"": ""int"" },
    { ""name"": ""data"", ""type"": {
        ""type"": ""record"",
        ""name"": ""confluent.type.Variant"",
        ""logicalType"": ""variant"",
        ""fields"": [
          { ""name"": ""metadata"", ""type"": ""bytes"" },
          { ""name"": ""value"", ""type"": ""bytes"" }
        ]
    } }
  ]
}";

        public VariantAvroSerdeTests() : base()
        {
        }

        [Fact]
        public void VariantField_RoundTripsAsVariant()
        {
            var serializer = new AvroSerializer<GenericRecord>(schemaRegistryClient, null);
            var deserializer = new AvroDeserializer<GenericRecord>(schemaRegistryClient, null);

            var schema = (RecordSchema)Avro.Schema.Parse(HolderSchema);
            var holder = new GenericRecord(schema);
            holder.Add("id", 7);
            holder.Add("data", Variant.ParseJson("{\"name\":\"alice\",\"age\":30}"));

            var ctx = new SerializationContext(MessageComponentType.Value, testTopic);
            var bytes = serializer.SerializeAsync(holder, ctx).Result;
            var result = deserializer.DeserializeAsync(bytes, false, ctx).Result;

            Assert.Equal(7, result["id"]);
            var variant = Assert.IsType<Variant>(result["data"]);
            Assert.Equal(VariantType.Object, variant.GetVariantType());
            Assert.Equal("alice", variant.GetFieldByKey("name").GetString());
            Assert.Equal(30, variant.GetFieldByKey("age").GetLong());
        }

        [Fact]
        public void VariantField_ScalarRoundTrips()
        {
            var serializer = new AvroSerializer<GenericRecord>(schemaRegistryClient, null);
            var deserializer = new AvroDeserializer<GenericRecord>(schemaRegistryClient, null);

            var schema = (RecordSchema)Avro.Schema.Parse(HolderSchema);
            var holder = new GenericRecord(schema);
            holder.Add("id", 1);
            holder.Add("data", Variant.ParseJson("\"hello\""));

            var ctx = new SerializationContext(MessageComponentType.Value, testTopic);
            var bytes = serializer.SerializeAsync(holder, ctx).Result;
            var result = deserializer.DeserializeAsync(bytes, false, ctx).Result;

            var variant = Assert.IsType<Variant>(result["data"]);
            Assert.Equal(VariantType.String, variant.GetVariantType());
            Assert.Equal("hello", variant.GetString());
        }
    }
}
