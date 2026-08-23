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
using System.Threading.Tasks;
using Avro;
using Avro.Generic;
using Confluent.SchemaRegistry.Rules;
using Google.Protobuf.WellKnownTypes;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     Tests for the CEL Decimal (<c>decimal(...)</c> / <c>decimals.*</c>) and Timestamp
    ///     (<c>timestamp.of</c>) function families, and for marshalling each of the four
    ///     schema-side decimal/timestamp shapes into CEL: an Avro logical timestamp, a
    ///     Protobuf WKT timestamp, an Avro logical decimal, and a Protobuf
    ///     <c>confluent.type.Decimal</c>.
    /// </summary>
    public class CelDecimalTimestampTests
    {
        private static ValidationRule Rule(string expr) => new ValidationRule { Name = "r", Expr = expr };

        private static Task<object> Eval(string expr, object value) =>
            new CelValidator().Execute(Rule(expr), null, value);

        // ---- Decimal operators (cross-client pinned values) ----

        [Theory]
        [InlineData("decimals.gt(decimal(\"12.34\"), decimal(\"10.00\"))", true)]
        [InlineData("decimals.lt(decimal(\"12.34\"), decimal(\"10.00\"))", false)]
        [InlineData("decimals.eq(decimal(\"1.50\"), decimal(\"1.5\"))", true)]
        [InlineData("decimals.eq(decimals.add(decimal(\"12.34\"), decimal(\"1.66\")), decimal(\"14.00\"))", true)]
        [InlineData("decimals.eq(decimals.sub(decimal(\"5\"), decimal(\"3\")), decimal(\"2\"))", true)]
        [InlineData("decimals.eq(decimals.mul(decimal(\"1.5\"), decimal(\"2\")), decimal(\"3.0\"))", true)]
        [InlineData("decimals.eq(decimals.mod(decimal(\"10\"), decimal(\"3\")), decimal(\"1\"))", true)]
        [InlineData("decimals.eq(decimals.sqrt(decimal(\"144\")), decimal(\"12\"))", true)]
        [InlineData("decimals.eq(decimals.greatest(decimal(\"2.5\"), decimal(\"9.99\")), decimal(\"9.99\"))", true)]
        [InlineData("decimals.eq(decimals.least(decimal(\"2.5\"), decimal(\"9.99\")), decimal(\"2.5\"))", true)]
        [InlineData("decimals.eq(decimals.neg(decimal(\"2.5\")), decimal(\"-2.5\"))", true)]
        [InlineData("decimals.eq(decimals.abs(decimal(\"-2.5\")), decimal(\"2.5\"))", true)]
        [InlineData("decimals.sign(decimal(\"-2.5\")) == -1", true)]
        [InlineData("double(decimal(\"100.50\")) == 100.5", true)]
        public async Task DecimalOperators(string expr, bool expected)
        {
            Assert.Equal(expected, await Eval(expr, 1));
        }

        [Theory]
        // Division: exact terminates, non-terminating rounds to 38 significant digits HALF_UP.
        [InlineData("string(decimals.div(decimal(\"1\"), decimal(\"8\")))", "0.125")]
        [InlineData("string(decimals.div(decimal(\"12\"), decimal(\"1\")))", "12")]
        [InlineData("string(decimals.div(decimal(\"1\"), decimal(\"99\")))",
            "0.010101010101010101010101010101010101010")]
        [InlineData("string(decimals.div(decimal(\"2\"), decimal(\"3\")))",
            "0.66666666666666666666666666666666666667")]
        // Exact division targets Java's preferred scale (dividend.scale - divisor.scale):
        // trailing zeros are kept down to it and padded up to it, never stripped below.
        [InlineData("string(decimals.div(decimal(\"6.0\"), decimal(\"3\")))", "2.0")]
        [InlineData("string(decimals.div(decimal(\"10.00\"), decimal(\"2\")))", "5.00")]
        [InlineData("string(decimals.div(decimal(\"-6.0\"), decimal(\"3\")))", "-2.0")]
        [InlineData("string(decimals.div(decimal(\"100.00\"), decimal(\"4\")))", "25.00")]
        [InlineData("string(decimals.div(decimal(\"6\"), decimal(\"3\")))", "2")]
        [InlineData("string(decimals.sqrt(decimal(\"2\")))", "1.4142135623730950488016887242096980786")]
        // Exact square root targets Java's preferred scale (radicand.scale / 2).
        [InlineData("string(decimals.sqrt(decimal(\"4.00\")))", "2.0")]
        [InlineData("string(decimals.sqrt(decimal(\"100.0000\")))", "10.00")]
        // Rounding family (Flink-aligned).
        [InlineData("string(decimals.round(decimal(\"2.567\"), 2))", "2.57")]
        [InlineData("string(decimals.trunc(decimal(\"2.567\"), 2))", "2.56")]
        [InlineData("string(decimals.floor(decimal(\"2.9\")))", "2")]
        [InlineData("string(decimals.ceil(decimal(\"2.1\")))", "3")]
        // string(Decimal) is plain (never scientific).
        [InlineData("string(decimal(\"1.50\"))", "1.50")]
        // decimal(double) keeps the whole-number ".0" (scale 1), matching Java valueOf / Python
        // str(float); the scale then carries through multiply (1+1 -> 2).
        [InlineData("string(decimal(2.0))", "2.0")]
        [InlineData("string(decimals.mul(decimal(2.0), decimal(0.5)))", "1.00")]
        public async Task DecimalStringForms(string expr, string expected)
        {
            Assert.Equal(expected, await Eval(expr, 1));
        }

        [Fact]
        public async Task DecimalFromBytesAndScale()
        {
            // 12.34 = unscaled 1234 (0x04D2) at scale 2.
            Assert.Equal(true, await Eval(
                "decimals.eq(decimal(b\"\\x04\\xd2\", 2), decimal(\"12.34\"))", 1));
        }

        [Theory]
        // A scale argument outside int32 range must error (matching Java's requireIntScale),
        // rather than silently taking the low 32 bits (e.g. 2^32 -> 0) and producing a wildly
        // wrong Decimal. 3_000_000_000 and -3_000_000_000 both exceed int32.
        [InlineData("decimals.round(decimal(\"2.5\"), 3000000000)")]
        [InlineData("decimals.trunc(decimal(\"2.5\"), 3000000000)")]
        [InlineData("decimals.round(decimal(\"2.5\"), -3000000000)")]
        [InlineData("decimal(b\"\\x04\\xd2\", 3000000000)")]
        public async Task ScaleOutOfIntRange_Throws(string expr)
        {
            await Assert.ThrowsAnyAsync<Exception>(() => Eval(expr, 1));
        }

        // ---- Marshalling: the four schema-side shapes into CEL ----

        [Fact]
        public async Task ProtoConfluentTypeDecimalIntoCel()
        {
            // A confluent.type.Decimal message (built via the serde-side extension).
            Confluent.SchemaRegistry.Serdes.Protobuf.Decimal dec = 12.34m.ToProtobufDecimal();
            Assert.Equal(true, await Eval("decimals.gt(decimal(this), decimal(\"10.00\"))", dec));
        }

        [Fact]
        public async Task ProtoWktTimestampIntoCel()
        {
            Timestamp ts = Timestamp.FromDateTime(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));
            // A WKT Timestamp marshals to a CEL timestamp, comparable with now.
            Assert.Equal(true, await Eval("this < now", ts));
        }

        [Fact]
        public async Task AvroLogicalDecimalIntoCel()
        {
            // Avro's `decimal` logical type decodes to AvroDecimal. A field-level rule binds
            // that value as `this` (as the field-rule walker does), and decimal(dyn) reads its
            // unscaled value + scale — mirroring the JVM client's decimal(this) on a decimal field.
            var amount = new AvroDecimal(12.34m);
            Assert.Equal(true, await Eval("decimals.gt(decimal(this), decimal(\"10.00\"))", amount));
        }

        [Fact]
        public async Task AvroLogicalDecimalRecordFieldIntoCel()
        {
            // Record field access: cel.net (>= 2.1.0) carries the AvroDecimal through the Avro
            // registry so `this.amount` reaches decimal(dyn) instead of throwing.
            var schema = (RecordSchema)Avro.Schema.Parse(@"{
                ""type"": ""record"", ""name"": ""DecimalRecord"",
                ""fields"": [ { ""name"": ""amount"",
                    ""type"": { ""type"": ""bytes"", ""logicalType"": ""decimal"",
                                ""precision"": 8, ""scale"": 2 } } ] }");
            var record = new GenericRecord(schema);
            record.Add("amount", new AvroDecimal(12.34m));
            Assert.Equal(true, await Eval("decimals.gt(decimal(this.amount), decimal(\"10.00\"))", record));
        }

        [Fact]
        public async Task AvroLogicalTimestampIntoCel()
        {
            var schema = (RecordSchema)Avro.Schema.Parse(@"{
                ""type"": ""record"", ""name"": ""TsRecord"",
                ""fields"": [ { ""name"": ""ts"",
                    ""type"": { ""type"": ""long"", ""logicalType"": ""timestamp-millis"" } } ] }");
            var record = new GenericRecord(schema);
            record.Add("ts", new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc));
            Assert.Equal(true, await Eval("timestamp.of(this.ts) < now", record));
        }
    }
}
