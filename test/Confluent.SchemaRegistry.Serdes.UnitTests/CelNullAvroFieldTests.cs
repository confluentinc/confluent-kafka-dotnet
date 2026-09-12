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
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry.Rules;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     A <c>CEL_FIELD</c> rule over the *null* branch of an Avro <c>["null", T]</c> union must
    ///     be evaluated, not skipped.
    ///
    ///     Avro's null is a first-class value, and the reference binds it as CEL null so a rule can
    ///     guard with <c>value == null</c>. Skipping the field instead removes that capability and
    ///     is *silent*: a rule that never ran and a rule that ran and passed produce the same
    ///     result, so nothing in a positive-only test can tell them apart.
    ///
    ///     Two things had to change together. The Avro walk no longer returns early on a null
    ///     value, so the union resolves to its null branch and the leaf reaches the rule. And
    ///     <c>FindType</c> declares a NullValue binding as the null type - it is a protobuf enum,
    ///     so it was declared as <c>int</c> and <c>value == null</c> failed at *check* time with
    ///     "no matching overload for '_==_' applied to '(int, null)'", before the rule ever ran.
    ///
    ///     The protobuf walk still skips an unset field, which is correct there: a field with
    ///     presence that is unset has no value, and writing one back would materialise it.
    /// </summary>
    public class CelNullAvroFieldTests : BaseSerializeDeserializeTests
    {
        private const string SchemaText = @"{
  ""type"": ""record"",
  ""name"": ""Nullable"",
  ""fields"": [
    {""name"": ""amount"",
     ""type"": [""null"", {""type"": ""bytes"", ""logicalType"": ""decimal"", ""precision"": 8, ""scale"": 2}],
     ""confluent:tags"": [""AMOUNT""]},
    {""name"": ""plain"", ""type"": ""string""}
  ]
}";

        public CelNullAvroFieldTests() : base()
        {
            CelExecutor.Register();
            CelFieldExecutor.Register();
        }

        /// <summary>Serializes a record whose decimal field is null; returns "" when the rule passed.</summary>
        private string Run(string subject, string expr, object amount)
        {
            var rule = new Rule("r", RuleKind.Condition, RuleMode.Write, "CEL_FIELD",
                new HashSet<string> { "AMOUNT" }, null, expr, null, null, false);
            var schema = new RegisteredSchema(subject + "-value", 1, 1, SchemaText,
                SchemaType.Avro, null)
            {
                RuleSet = new RuleSet(new List<Rule>(), new List<Rule> { rule })
            };
            store[SchemaText] = 1;
            subjectStore[subject + "-value"] = new List<RegisteredSchema> { schema };

            var rec = new GenericRecord((RecordSchema)Avro.Schema.Parse(SchemaText));
            rec.Add("amount", amount);
            rec.Add("plain", "hi");

            var ser = new AvroSerializer<GenericRecord>(schemaRegistryClient,
                new AvroSerializerConfig { AutoRegisterSchemas = false, UseLatestVersion = true });
            try
            {
                ser.SerializeAsync(rec,
                    new SerializationContext(MessageComponentType.Value, subject)).Wait();
                return "";
            }
            catch (Exception e)
            {
                var inner = e;
                while (inner.InnerException != null) inner = inner.InnerException;
                return inner.Message;
            }
        }

        [Fact]
        public void NullFieldReachesTheRule()
        {
            // `value == null` can only pass if the null was bound and the rule ran.
            Assert.Equal("", Run("nf1", "value == null", null));
        }

        [Fact]
        public void NullFieldWasNotMerelySkipped()
        {
            // The discriminator: `value != null` is false on a null, so it must FAIL. Without it
            // the test above is satisfied by a rule that never ran - a skipped field reports
            // nothing either.
            Assert.Contains("Expr failed", Run("nf2", "value != null", null));
        }

        [Fact]
        public void UnguardedRuleOnANullFailsLoudly()
        {
            Assert.Contains("Cannot convert",
                Run("nf3", "decimals.gt(decimal(value), decimal(\"10.00\"))", null));
        }

        [Fact]
        public void GuardedRuleOnANullPasses()
        {
            Assert.Equal("", Run("nf4",
                "value == null || decimals.gt(decimal(value), decimal(\"10.00\"))", null));
        }

        // ---- writing a CEL null back into a ["null", T] union -------------------------------
        //
        // A message-level CEL transform hands its result back as a map, and a null in that map is
        // Cel.NET's NullValue - a protobuf enum Avro's writer rejects with "Cannot find a match
        // for ...NullValue". That hit an identity pass-through over a nullable field and the
        // `has(x) ? x : null` guard, the only way to preserve absence.
        //
        // Asserted on the *deserialized* record: "the serializer did not throw" cannot tell a
        // preserved null from a materialised zero, since writing either into a union encodes fine.
        private GenericRecord RoundTripTransform(string subject, string expr)
        {
            var rule = new Rule("r", RuleKind.Transform, RuleMode.Write, "CEL",
                null, null, expr, null, null, false);
            var schema = new RegisteredSchema(subject + "-value", 1, 1, SchemaText,
                SchemaType.Avro, null)
            {
                RuleSet = new RuleSet(new List<Rule>(), new List<Rule> { rule })
            };
            store[SchemaText] = 1;
            subjectStore[subject + "-value"] = new List<RegisteredSchema> { schema };

            var rec = new GenericRecord((RecordSchema)Avro.Schema.Parse(SchemaText));
            rec.Add("amount", null);
            rec.Add("plain", "hi");

            var ser = new AvroSerializer<GenericRecord>(schemaRegistryClient,
                new AvroSerializerConfig { AutoRegisterSchemas = false, UseLatestVersion = true });
            var deser = new AvroDeserializer<GenericRecord>(schemaRegistryClient, null);
            var ctx = new SerializationContext(MessageComponentType.Value, subject);
            byte[] bytes = ser.SerializeAsync(rec, ctx).Result;
            return deser.DeserializeAsync(bytes, false, ctx).Result;
        }

        [Fact]
        public void PassThroughPreservesANullField()
        {
            var got = RoundTripTransform("mt1",
                "{'amount': message.amount, 'plain': message.plain}");
            Assert.Null(got["amount"]);
            Assert.Equal("hi", got["plain"]);
        }

        [Fact]
        public void GuardedTransformPreservesANullField()
        {
            var got = RoundTripTransform("mt2",
                "{'amount': has(message.amount) ? message.amount : null, 'plain': message.plain}");
            Assert.Null(got["amount"]);
        }

        [Fact]
        public void TransformCanStillSetTheNullableField()
        {
            // The twin: "the null survived" must not be "nothing was written at all".
            var got = RoundTripTransform("mt3",
                "{'amount': decimal('7.50'), 'plain': message.plain}");
            Assert.Equal(new AvroDecimal(7.50m), got["amount"]);
        }

        // ---- a field-level TRANSFORM over the null branch ----------------------------------
        //
        // CelFieldExecutor keyed its inverse logical-type conversions off `fieldValue is
        // AvroDecimal`, which is false for the null branch, so a rule materialising a decimal
        // from null handed Avro a BigDecimal it cannot write. One rule, two inputs, and only
        // the null one failed.
        private string FieldTransform(string subject, string expr, object amount)
        {
            var rule = new Rule("r", RuleKind.Transform, RuleMode.Write, "CEL_FIELD",
                new HashSet<string> { "AMOUNT" }, null, expr, null, null, false);
            var schema = new RegisteredSchema(subject + "-value", 1, 1, SchemaText,
                SchemaType.Avro, null)
            {
                RuleSet = new RuleSet(new List<Rule>(), new List<Rule> { rule })
            };
            store[SchemaText] = 1;
            subjectStore[subject + "-value"] = new List<RegisteredSchema> { schema };

            var rec = new GenericRecord((RecordSchema)Avro.Schema.Parse(SchemaText));
            rec.Add("amount", amount);
            rec.Add("plain", "hi");

            var ser = new AvroSerializer<GenericRecord>(schemaRegistryClient,
                new AvroSerializerConfig { AutoRegisterSchemas = false, UseLatestVersion = true });
            var deser = new AvroDeserializer<GenericRecord>(schemaRegistryClient, null);
            var ctx = new SerializationContext(MessageComponentType.Value, subject);
            byte[] bytes = ser.SerializeAsync(rec, ctx).Result;
            GenericRecord got = deser.DeserializeAsync(bytes, false, ctx).Result;
            return got["amount"] == null ? "<null>" : got["amount"].ToString();
        }

        [Fact]
        public void FieldTransformMaterialisesADecimalFromANull()
        {
            Assert.Equal(new AvroDecimal(7.50m).ToString(),
                FieldTransform("ft1", "decimal('7.50')", null));
        }

        [Fact]
        public void FieldTransformOnAPresentDecimalIsUnchanged()
        {
            // The control that localised it: the same rule always worked here.
            Assert.Equal(new AvroDecimal(7.50m).ToString(),
                FieldTransform("ft2", "decimal('7.50')", new AvroDecimal(1.00m)));
        }

        [Fact]
        public void FieldTransformCanStillEchoANull()
        {
            // The null branch still round-trips: Cel.NET binds it as the protobuf NullValue
            // enum, which FindType declares as CheckedNull, so an identity rule hands back null.
            Assert.Equal("<null>", FieldTransform("ft3", "value", null));
        }

        [Fact]
        public void PresentValueStillEvaluatesNormally()
        {
            // The must-pass / must-fail pair: removing the skip must not disturb the ordinary case.
            var present = new AvroDecimal(12.34m);
            Assert.Equal("", Run("nf5",
                "decimals.gt(decimal(value), decimal(\"10.00\"))", present));
            Assert.Contains("Expr failed", Run("nf6",
                "decimals.gt(decimal(value), decimal(\"100.00\"))", present));
        }
    }
}
