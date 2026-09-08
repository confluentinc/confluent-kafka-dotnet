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

using System.Collections.Generic;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     Message-level <c>CEL</c> transforms over Avro, driven end to end through the
    ///     serializer.
    ///
    ///     Two defects met here. Reading <c>message.data</c> threw
    ///     <c>Cannot get schema for Confluent.SchemaRegistry.Variant</c> (wrapped in the
    ///     registry's <c>"oops"</c> placeholder), because the CEL boundary had an arm for an Avro
    ///     decimal and none for a variant. And the result came back as a
    ///     plain dictionary, which the Avro writer cannot encode.
    ///
    ///     Driving the real serializer is deliberate: the second defect was in encoding, so a
    ///     test that stopped at the executor's return value would not have caught it.
    /// </summary>
    public class CelAvroMessageTransformTests : BaseSerializeDeserializeTests
    {
        private const string ValueTypesSchema = @"
{
  ""type"": ""record"",
  ""name"": ""ValueTypes"",
  ""fields"": [
    { ""name"": ""amount"", ""type"": {
        ""type"": ""bytes"", ""logicalType"": ""decimal"",
        ""precision"": 8, ""scale"": 2 } },
    { ""name"": ""ts"", ""type"": {
        ""type"": ""long"", ""logicalType"": ""timestamp-millis"" } },
    { ""name"": ""data"", ""type"": {
        ""type"": ""record"",
        ""name"": ""confluent.type.Variant"",
        ""logicalType"": ""variant"",
        ""fields"": [
          { ""name"": ""metadata"", ""type"": ""bytes"" },
          { ""name"": ""value"", ""type"": ""bytes"" }
        ] } },
    { ""name"": ""label"", ""type"": ""string"" }
  ]
}";

        public CelAvroMessageTransformTests() : base()
        {
        }

        private static GenericRecord Record()
        {
            var schema = (RecordSchema)Avro.Schema.Parse(ValueTypesSchema);
            var record = new GenericRecord(schema);
            // 12.34, the same fixture the other clients use.
            record.Add("amount", new AvroDecimal(1234, 2));
            record.Add("ts", new System.DateTime(2023, 11, 14, 22, 13, 20, 123,
                System.DateTimeKind.Utc));
            record.Add("data", Variant.ParseJson("{\"name\":\"alice\"}"));
            record.Add("label", "hi");
            return record;
        }

        /// <summary>Registers the schema with one message-level CEL transform, then round trips.</summary>
        private GenericRecord RoundTrip(string expr)
        {
            return RoundTrip(ValueTypesSchema, Record(), expr);
        }

        private GenericRecord RoundTrip(string schemaText, GenericRecord input, string expr)
        {
            var rule = new Rule("test-cel", RuleKind.Transform, RuleMode.Write, "CEL", null,
                null, expr, null, null, false);
            var schema = new RegisteredSchema(testTopic + "-value", 1, 1, schemaText,
                SchemaType.Avro, null)
            {
                RuleSet = new RuleSet(new List<Rule>(), new List<Rule> { rule })
            };
            store[schemaText] = 1;
            subjectStore[testTopic + "-value"] = new List<RegisteredSchema> { schema };

            var serConfig = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false, UseLatestVersion = true
            };
            var serializer = new AvroSerializer<GenericRecord>(schemaRegistryClient, serConfig);
            var deserializer = new AvroDeserializer<GenericRecord>(schemaRegistryClient, null);

            var ctx = new SerializationContext(MessageComponentType.Value, testTopic);
            var bytes = serializer.SerializeAsync(input, ctx).Result;
            return deserializer.DeserializeAsync(bytes, false, ctx).Result;
        }

        private const string AllFields =
            "'amount': message.amount, 'ts': message.ts, " +
            "'data': message.data, 'label': message.label";

        /// <summary>
        ///     An identity transform is the cheapest regression test for a write-back path, and
        ///     here it also covers the read: it fails if a variant field cannot be read at all.
        /// </summary>
        [Fact]
        public void PassThroughRoundTrips()
        {
            var result = RoundTrip("{" + AllFields + "}");

            Assert.Equal(new AvroDecimal(1234, 2), result["amount"]);
            var variant = Assert.IsType<Variant>(result["data"]);
            Assert.Equal("alice", variant.GetFieldByKey("name").GetString());
            Assert.Equal("hi", result["label"]);
        }

        [Fact]
        public void ComputedDecimalIsWrittenBack()
        {
            var result = RoundTrip(
                "{'amount': decimals.add(decimal(message.amount), decimal('1.00')), " +
                "'ts': message.ts, 'data': message.data, 'label': message.label}");

            Assert.Equal(new AvroDecimal(1334, 2), result["amount"]);
        }

        [Fact]
        public void ComputedTimestampIsWrittenBack()
        {
            var result = RoundTrip(
                "{'amount': message.amount, 'ts': message.ts + duration('60s'), " +
                "'data': message.data, 'label': message.label}");

            var ts = Assert.IsType<System.DateTime>(result["ts"]);
            Assert.Equal(new System.DateTime(2023, 11, 14, 22, 14, 20, 123,
                System.DateTimeKind.Utc), ts.ToUniversalTime());
        }

        /// <summary>
        ///     Asserted through the decoded field rather than the metadata bytes: metadata holds
        ///     the field names, so the two documents share it and comparing metadata would prove
        ///     nothing.
        /// </summary>
        [Fact]
        public void ComputedVariantIsWrittenBack()
        {
            var result = RoundTrip(
                "{'amount': message.amount, 'ts': message.ts, " +
                "'data': variants.parseJson('{\"name\":\"bob\"}'), 'label': message.label}");

            var variant = Assert.IsType<Variant>(result["data"]);
            Assert.Equal("bob", variant.GetFieldByKey("name").GetString());
        }

        /// <summary>
        ///     Replace, not merge: the rule's map is the whole new record, so a field the rule
        ///     does not name is gone rather than carried over from the input.
        ///
        ///     This case existed only on the protobuf side, and its absence hid a real defect
        ///     elsewhere - the C++ client seeded its result record from the input before applying
        ///     the map, so it merged. Every other case here names <i>all</i> of the record's
        ///     fields, which makes merge and replace indistinguishable; a positive-only fixture
        ///     cannot tell "wrote the right thing" from "did not need to".
        ///
        ///     Asserted as "the round trip fails" rather than on the exception type, because the
        ///     type is the thing worth changing: Apache.Avro's writer raises a bare
        ///     <c>NullReferenceException</c> for a field the record does not carry, where the JVM
        ///     raises <c>AvroRuntimeException</c> naming the field. Under merge this call would
        ///     succeed and return the input's 12.34, so the assertion holds either way.
        ///
        ///     <see cref="PassThroughRoundTrips" /> is the must-fail twin: naming every field
        ///     still round trips, so "throws" cannot mean "the transform stopped working".
        /// </summary>
        [Fact]
        public void AFieldTheRuleDoesNotNameIsDropped()
        {
            var ex = Assert.ThrowsAny<System.Exception>(
                () => RoundTrip("{'label': message.label}"));

            // Naming the field is the point. This used to surface as a bare
            // NullReferenceException from inside Apache.Avro's encode.
            var inner = ex;
            while (inner.InnerException != null)
            {
                inner = inner.InnerException;
            }

            Assert.Contains("amount", inner.Message);
            Assert.Contains("no default value", inner.Message);
            Assert.IsNotType<System.NullReferenceException>(inner);
        }

        private const string DefaultsSchema = @"
{
  ""type"": ""record"",
  ""name"": ""Defaults"",
  ""fields"": [
    { ""name"": ""kept"", ""type"": ""string"" },
    { ""name"": ""withDefault"", ""type"": ""string"", ""default"": ""fallback"" },
    { ""name"": ""nullable"", ""type"": [ ""null"", ""string"" ], ""default"": null }
  ]
}";

        /// <summary>
        ///     An unnamed field takes its schema's declared default, matching
        ///     GenericRecordBuilder.build() on the JVM. The value it must <i>not</i> take is the one
        ///     it had on the way in - that would be merge, which is what the C++ client used to do.
        ///
        ///     Before this, the default was never consulted: BuildRecord walked the rule's map and
        ///     never the schema's fields, so a field with a perfectly good default failed exactly
        ///     like one without.
        /// </summary>
        [Fact]
        public void AnUnnamedFieldTakesItsDeclaredDefault()
        {
            var schema = (RecordSchema)Avro.Schema.Parse(DefaultsSchema);
            var input = new GenericRecord(schema);
            input.Add("kept", "original-kept");
            input.Add("withDefault", "original-withDefault");
            input.Add("nullable", "original-nullable");

            var result = RoundTrip(DefaultsSchema, input, "{'kept': message.kept}");

            Assert.Equal("original-kept", result["kept"]);
            Assert.Equal("fallback", result["withDefault"]);
            Assert.NotEqual("original-withDefault", result["withDefault"]);
            Assert.Null(result["nullable"]);
        }

    }
}
