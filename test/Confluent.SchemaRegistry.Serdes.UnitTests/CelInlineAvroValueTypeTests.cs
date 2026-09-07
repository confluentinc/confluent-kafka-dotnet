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

using System;
using System.Collections.Generic;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry.Rules;
using Confluent.SchemaRegistry.Serdes;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     Inline <c>confluent:rules</c> over Avro's value types - decimal, timestamp and variant -
    ///     at both message and field level.
    ///
    ///     This is this's inline half. A message-level rule already worked: <c>this</c> is the
    ///     record, and field access on it goes through the Avro registry adapter, which converts
    ///     each value. A <b>field</b>-level rule binds the value straight to <c>this</c>, missing
    ///     that adapter entirely, so an <c>AvroDecimal</c> reached the checker as
    ///     <c>Avro.AvroDecimal</c> and a timestamp as <c>System.DateTime</c> - neither of which the
    ///     CEL overloads accept, giving <c>found no matching overload</c> at <i>check</i> time,
    ///     before the rule ever ran.
    ///
    ///     Field-level variant escaped it, because a <c>Variant</c> is carried opaquely rather than
    ///     through an overload - the same single-cell survivor JS had before its inline path was fixed.
    ///
    ///     Twelve cases: six positives, and the must-fail twin of each. The twins are what make the
    ///     positives worth anything - a rule that never runs also reports no violation.
    /// </summary>
    public class CelInlineAvroValueTypeTests : BaseSerializeDeserializeTests
    {
        public CelInlineAvroValueTypeTests() : base()
        {
            CelValidator.Register();
            CelFieldExecutor.Register();
        }

        /// <summary>
        ///     The fixture is 12.34 / 1700000000.123 / {"name":"alice"}. <paramref name="pos" />
        ///     picks rules that must all hold; its inverse picks rules that must all fail.
        /// </summary>
        private static string SchemaFor(bool pos)
        {
            string dec = pos ? "decimal('10.00')" : "decimal('1000.00')";
            string ts = pos ? "'2000-01-01T00:00:00Z'" : "'2050-01-01T00:00:00Z'";
            string varType = pos ? "'object'" : "'array'";
            string n = pos ? "" : "N";
            return @"
{
  ""type"": ""record"",
  ""name"": ""ValueTypes"",
  ""fields"": [
    { ""name"": ""amount"",
      ""type"": { ""type"": ""bytes"", ""logicalType"": ""decimal"", ""precision"": 8, ""scale"": 2 },
      ""confluent:rules"": [ { ""name"": ""fldDec" + n + @""", ""expr"": ""decimals.gt(this, " + dec + @")"" } ] },
    { ""name"": ""ts"",
      ""type"": { ""type"": ""long"", ""logicalType"": ""timestamp-millis"" },
      ""confluent:rules"": [ { ""name"": ""fldTs" + n + @""", ""expr"": ""this > timestamp(" + ts + @")"" } ] },
    { ""name"": ""data"",
      ""type"": { ""type"": ""record"", ""name"": ""confluent.type.Variant"", ""logicalType"": ""variant"",
                 ""fields"": [ { ""name"": ""metadata"", ""type"": ""bytes"" },
                              { ""name"": ""value"", ""type"": ""bytes"" } ] },
      ""confluent:rules"": [ { ""name"": ""fldVar" + n + @""", ""expr"": ""variants.type(this) == " + varType + @""" } ] },
    { ""name"": ""plain"", ""type"": ""string"" }
  ],
  ""confluent:rules"": [
    { ""name"": ""msgDec" + n + @""", ""expr"": ""decimals.gt(this.amount, " + dec + @")"" },
    { ""name"": ""msgTs" + n + @""", ""expr"": ""this.ts > timestamp(" + ts + @")"" },
    { ""name"": ""msgVar" + n + @""", ""expr"": ""variants.type(this.data) == " + varType + @""" }
  ]
}";
        }

        private static GenericRecord Record(string schemaText)
        {
            var record = new GenericRecord((RecordSchema)Avro.Schema.Parse(schemaText));
            record.Add("amount", new AvroDecimal(1234, 2));
            record.Add("ts", new DateTime(2023, 11, 14, 22, 13, 20, 123, DateTimeKind.Utc));
            record.Add("data", Variant.ParseJson("{\"name\":\"alice\"}"));
            record.Add("plain", "hi");
            return record;
        }

        /// <summary>Serializes the fixture under one schema's inline rules; "" when clean.</summary>
        private string Violations(string subject, string schemaText)
        {
            var schema = new RegisteredSchema(subject + "-value", 1, 1, schemaText,
                SchemaType.Avro, null);
            store[schemaText] = 1;
            subjectStore[subject + "-value"] = new List<RegisteredSchema> { schema };

            var serializer = new AvroSerializer<GenericRecord>(schemaRegistryClient,
                new AvroSerializerConfig
                {
                    AutoRegisterSchemas = false,
                    UseLatestVersion = true,
                    ValidationRulesExecution = ValidationRulesExecution.AfterDomainRules
                });
            try
            {
                serializer.SerializeAsync(Record(schemaText),
                    new SerializationContext(MessageComponentType.Value, subject)).Wait();
                return "";
            }
            catch (Exception e)
            {
                var inner = e;
                while (inner.InnerException != null)
                {
                    inner = inner.InnerException;
                }

                return inner.Message;
            }
        }

        [Fact]
        public void BindsEveryValueTypeSoAllSixPositiveRulesHold()
        {
            // Before the fix this reported two violations - fldDec and fldTs - each a
            // "found no matching overload" raised at check time.
            Assert.Equal("", Violations("inline-pos", SchemaFor(true)));
        }

        /// <summary>
        ///     A tagged <c>CEL_FIELD</c> rule on an Avro timestamp field. The
        ///     field path binds through the same ToCelValue, so it failed the same way - at check
        ///     time, on the declared type, with the runtime value perfectly correct. Decimal
        ///     already worked here, which is what isolated the defect to the declaration.
        /// </summary>
        [Theory]
        [InlineData("value > timestamp('2000-01-01T00:00:00Z')", "CONDITION")]
        [InlineData("value + duration('60s')", "TRANSFORM")]
        public void FieldRulesReachAnAvroTimestamp(string expr, string kind)
        {
            string schemaText = @"
{
  ""type"": ""record"",
  ""name"": ""ValueTypes"",
  ""fields"": [
    { ""name"": ""ts"",
      ""type"": { ""type"": ""long"", ""logicalType"": ""timestamp-millis"" },
      ""confluent:tags"": [ ""TS"" ] },
    { ""name"": ""plain"", ""type"": ""string"" }
  ]
}";
            var rule = new Rule("r", kind == "CONDITION" ? RuleKind.Condition : RuleKind.Transform,
                RuleMode.Write, "CEL_FIELD", new HashSet<string> { "TS" }, null, expr, null, null,
                false);
            var schema = new RegisteredSchema("fieldts-value", 1, 1, schemaText, SchemaType.Avro,
                null)
            {
                RuleSet = new RuleSet(new List<Rule>(), new List<Rule> { rule })
            };
            store[schemaText] = 1;
            subjectStore["fieldts-value"] = new List<RegisteredSchema> { schema };

            var record = new GenericRecord((RecordSchema)Avro.Schema.Parse(schemaText));
            record.Add("ts", new DateTime(2023, 11, 14, 22, 13, 20, 123, DateTimeKind.Utc));
            record.Add("plain", "hi");

            var serializer = new AvroSerializer<GenericRecord>(schemaRegistryClient,
                new AvroSerializerConfig { AutoRegisterSchemas = false, UseLatestVersion = true });
            var deserializer = new AvroDeserializer<GenericRecord>(schemaRegistryClient, null);
            var ctx = new SerializationContext(MessageComponentType.Value, "fieldts");

            // Before the fix both raised "found no matching overload" at check time.
            var bytes = serializer.SerializeAsync(record, ctx).Result;
            var result = deserializer.DeserializeAsync(bytes, false, ctx).Result;

            var expected = kind == "CONDITION"
                ? new DateTime(2023, 11, 14, 22, 13, 20, 123, DateTimeKind.Utc)
                : new DateTime(2023, 11, 14, 22, 14, 20, 123, DateTimeKind.Utc);
            Assert.Equal(expected, result["ts"]);
        }

        [Fact]
        public void FiresAllSixMustFailTwins()
        {
            string result = Violations("inline-neg", SchemaFor(false));

            Assert.Contains("6 violations", result);
            // Named individually, so a count that is six for the wrong reason still fails.
            foreach (string rule in new[]
                     { "fldDecN", "fldTsN", "fldVarN", "msgDecN", "msgTsN", "msgVarN" })
            {
                Assert.Contains(rule, result);
            }
        }
    }
}
