// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry.Rules;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     Writing a rule's result back over a schema with an **array** or a **map** field
    ///    .
    ///
    ///     Apache.Avro's generic writer type-checks the value against the schema: it wants a
    ///     <c>System.Array</c> for an array and an <c>IDictionary&lt;string, object&gt;</c> for a
    ///     map, and rejects a <c>List&lt;object&gt;</c> or a <c>Dictionary&lt;object, object&gt;</c>
    ///     outright. Four separate places produced the wrong shape, so *every* rule writing back
    ///     over such a schema failed whatever it computed - a field transform on the array (C5) and
    ///     a message transform naming it (C6, C7) alike.
    ///
    ///     Round-tripped through the real serializer and deserializer, because the failure was in
    ///     the writer: asserting on the in-memory result would have passed throughout.
    /// </summary>
    public class CelAvroContainerWriteBackTests : BaseSerializeDeserializeTests
    {
        private const string SchemaText = @"{
  ""type"": ""record"",
  ""name"": ""C9"",
  ""fields"": [
    {""name"": ""amounts"",
     ""type"": {""type"": ""array"", ""items"": {""type"": ""bytes"", ""logicalType"": ""decimal"", ""precision"": 8, ""scale"": 2}},
     ""confluent:tags"": [""AMOUNTS""]},
    {""name"": ""amountMap"",
     ""type"": {""type"": ""map"", ""values"": {""type"": ""bytes"", ""logicalType"": ""decimal"", ""precision"": 8, ""scale"": 2}}},
    {""name"": ""label"", ""type"": ""string""}
  ]
}";

        public CelAvroContainerWriteBackTests() : base()
        {
            CelExecutor.Register();
            CelFieldExecutor.Register();
        }

        private static GenericRecord Record()
        {
            var rs = (RecordSchema)Avro.Schema.Parse(SchemaText);
            var rec = new GenericRecord(rs);
            rec.Add("amounts", new object[] { new AvroDecimal(111, 2), new AvroDecimal(222, 2) });
            rec.Add("amountMap",
                new Dictionary<string, object> { { "a", new AvroDecimal(333, 2) } });
            rec.Add("label", "hi");
            return rec;
        }

        private GenericRecord RoundTrip(string subject, string expr, RuleKind kind, string tag)
        {
            var rule = new Rule("r", kind, RuleMode.Write, tag == null ? "CEL" : "CEL_FIELD",
                tag == null ? null : new HashSet<string> { tag }, null, expr, null, null, false);
            var schema = new RegisteredSchema(subject + "-value", 1, 1, SchemaText,
                SchemaType.Avro, null)
            {
                RuleSet = new RuleSet(new List<Rule>(), new List<Rule> { rule })
            };
            store[SchemaText] = 1;
            subjectStore[subject + "-value"] = new List<RegisteredSchema> { schema };

            var ser = new AvroSerializer<GenericRecord>(schemaRegistryClient,
                new AvroSerializerConfig { AutoRegisterSchemas = false, UseLatestVersion = true });
            var deser = new AvroDeserializer<GenericRecord>(schemaRegistryClient, null);
            var ctx = new SerializationContext(MessageComponentType.Value, subject);
            return deser.DeserializeAsync(ser.SerializeAsync(Record(), ctx).Result, false, ctx)
                .Result;
        }

        private static List<string> Amounts(GenericRecord r) =>
            ((IEnumerable<object>)r["amounts"]).Select(x => ((AvroDecimal)x).ToString()).ToList();

        private static string MapValue(GenericRecord r) =>
            ((AvroDecimal)((IDictionary<string, object>)r["amountMap"])["a"]).ToString();

        [Fact]
        public void FieldTransformOverAnArrayIsWrittenBack()
        {
            var got = RoundTrip("c9arr", "decimals.add(decimal(value), decimal(\"1.00\"))",
                RuleKind.Transform, "AMOUNTS");

            Assert.Equal(new List<string> { "2.11", "3.22" }, Amounts(got));
        }

        [Fact]
        public void MessageTransformKeepsTheArrayAndTheMap()
        {
            var got = RoundTrip("c9pass",
                "{'amounts': message.amounts, 'amountMap': message.amountMap, " +
                "'label': message.label}", RuleKind.Transform, null);

            Assert.Equal(new List<string> { "1.11", "2.22" }, Amounts(got));
            Assert.Equal("3.33", MapValue(got));
        }

        [Fact]
        public void MessageTransformCanComputeAnArrayElement()
        {
            // The twin for the pass-through: "the containers survived" must not be "nothing was
            // written", so a computed array has to land as well.
            var got = RoundTrip("c9elem",
                "{'amounts': [decimal('9.99')], 'amountMap': message.amountMap, " +
                "'label': message.label}", RuleKind.Transform, null);

            Assert.Equal(new List<string> { "9.99" }, Amounts(got));
            Assert.Equal("3.33", MapValue(got));
        }
    }
}
