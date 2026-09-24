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
    ///     (<c>timestamp</c>) function families, and for marshalling each of the four
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
        // The CEL == / != operators on two Decimal values are NUMERIC (value-equal,
        // scale-insensitive), matching decimals.eq. This routes DecimalT.Equal through
        // BigDecimal.Equals (CompareTo == 0), not scale-sensitive struct equality.
        [InlineData("decimal(\"2.0\") == decimal(\"2.00\")", true)]
        [InlineData("decimal(\"2.0\") == decimal(\"2.0\")", true)]
        [InlineData("decimal(\"2.0\") == decimal(\"2.1\")", false)]
        [InlineData("decimal(\"2.0\") != decimal(\"2.00\")", false)]
        [InlineData("decimal(\"2.0\") != decimal(\"2.1\")", true)]
        public async Task DecimalOperators(string expr, bool expected)
        {
            Assert.Equal(expected, await Eval(expr, 1));
        }

        [Theory]
        // decimal(this) == decimal(literal) is numeric: the receiver's scale (2.00, scale 2)
        // does not affect equality with 2.0 (scale 1) / 2 (scale 0).
        [InlineData("decimal(this) == decimal(\"2.00\")", true)]
        [InlineData("decimal(this) == decimal(\"2\")", true)]
        [InlineData("decimal(this) == decimal(\"2.5\")", false)]
        [InlineData("decimal(this) != decimal(\"2.5\")", true)]
        public async Task DecimalEqualityOnReceiver(string expr, bool expected)
        {
            // Receiver decodes to a scale-2 Decimal (2.00); equality must ignore that scale.
            Assert.Equal(expected, await Eval(expr, new AvroDecimal(2.00m)));
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

        // ---- timestamp(<bare int>) is epoch SECONDS (cross-client contract) ----

        [Theory]
        // CROSS-CLIENT CONTRACT: the standard CEL `timestamp(<int>)` conversion reads a bare
        // integer as epoch SECONDS (never millis), matching cel-go / cel-java / cel-cpp. In
        // Cel.NET this is the `int64_to_timestamp` overload declared in Checker/Standard.cs,
        // implemented by IntT.ConvertToType via Instant.FromUnixTimeSeconds. Pinned here so
        // the unit can't drift to millis.
        [InlineData("timestamp(1700000000) == timestamp(\"2023-11-14T22:13:20Z\")", true)]
        // ...and it is NOT the millis reading of the same integer
        // (1700000000 ms would be 1970-01-20T16:13:20Z).
        [InlineData("timestamp(1700000000) == timestamp(\"1970-01-20T16:13:20Z\")", false)]
        // Component accessors and the round-trip back to int agree.
        [InlineData("timestamp(1700000000).getFullYear() == 2023", true)]
        [InlineData("int(timestamp(\"2023-11-14T22:13:20Z\")) == 1700000000", true)]
        // Negative / pre-epoch ints run backwards from the epoch in seconds.
        [InlineData("timestamp(-1) == timestamp(\"1969-12-31T23:59:59Z\")", true)]
        [InlineData("timestamp(-86400) == timestamp(\"1969-12-31T00:00:00Z\")", true)]
        [InlineData("timestamp(0) == timestamp(\"1970-01-01T00:00:00Z\")", true)]
        // The two-argument timestamp(value, precision) form is unaffected: each precision
        // still scales as named, and precision 3 on the x1000 value lands on the same instant.
        [InlineData("timestamp(1700000000, 0) == timestamp(1700000000)", true)]
        [InlineData("timestamp(1700000000000, 3) == timestamp(1700000000)", true)]
        [InlineData("timestamp(1700000000000000, 6) == timestamp(1700000000)", true)]
        [InlineData("timestamp(1700000000000000000, 9) == timestamp(1700000000)", true)]
        // Sub-second precision survives, and the same integer differs across the two arities.
        [InlineData("timestamp(1700000000123, 3) == timestamp(\"2023-11-14T22:13:20.123Z\")", true)]
        [InlineData("timestamp(1700000000, 3) == timestamp(1700000000)", false)]
        public async Task TimestampBareIntIsEpochSeconds(string expr, bool expected)
        {
            Assert.Equal(expected, await Eval(expr, 1));
        }

        [Fact]
        public async Task TimestampBareIntEpochSecondsStringForm()
        {
            Assert.Equal(true, await Eval(
                "string(timestamp(1700000000)) == \"2023-11-14T22:13:20Z\"", 1));
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(4)]
        [InlineData(7)]
        [InlineData(10)]
        [InlineData(-3)]
        public async Task TimestampRejectsPrecisionOutsideTheSet(int precision)
        {
            // With the unit a number rather than a name, rejecting anything outside
            // {0, 3, 6, 9} is the only thing between a typo and a silently wrong instant.
            await Assert.ThrowsAnyAsync<Exception>(
                () => Eval($"timestamp(1700000000, {precision}) == timestamp(0)", 1));
        }

        [Fact]
        public async Task TimestampOfNamespaceIsGone()
        {
            await Assert.ThrowsAnyAsync<Exception>(
                () => Eval("timestamp.of(1700000000000, 3) == timestamp(0)", 1));
        }

        // ---- Marshalling: the four schema-side shapes into CEL ----

        [Fact]
        public async Task ProtoConfluentTypeDecimalIntoCel()
        {
            // A confluent.type.Decimal message (built via the serde-side extension).
            Confluent.SchemaRegistry.Serdes.Protobuf.Decimal dec = 12.34m.ToProtobufDecimal();
            Assert.Equal(true, await Eval("decimals.gt(decimal(this), decimal(\"10.00\"))", dec));
        }

        /// <summary>
        ///     A decimal reached by <b>selection</b> compares numerically, exactly as one bound
        ///     directly does. A boundary conversion cannot achieve this on its own -
        ///     <c>this.a</c> is resolved inside cel.net, past any boundary - so the registry
        ///     carries <c>ProtoValueToCel</c> and a <c>confluent.type.Decimal</c> becomes a
        ///     <see cref="DecimalT" /> wherever it appears, fields included. Without it cel.net
        ///     answers <c>==</c> with <c>lhs.Equal(rhs)</c> on the raw message, comparing unscaled
        ///     bytes and scale field by field, and called 1.50 and 1.5 unequal.
        /// </summary>
        [Fact]
        public async Task NestedProtoDecimalEquality()
        {
            // 1.50 and 1.5 - the same number in two encodings.
            var msg = new Example.NestedDecimals
            {
                A = 1.50m.ToProtobufDecimal(),
                B = 1.5m.ToProtobufDecimal()
            };

            // Accessors through a selection.
            Assert.Equal(true, await Eval("decimals.eq(this.a, this.b)", msg));
            Assert.Equal(true, await Eval("decimals.eq(decimal(this.a), decimal(this.b))", msg));
            // `==` through a selection.
            Assert.Equal(true, await Eval("this.a == this.b", msg));
            Assert.Equal(false, await Eval("this.a != this.b", msg));
            Assert.Equal(true, await Eval("this.a == this.a", msg));
            // Mixed with a constructed decimal.
            Assert.Equal(true, await Eval("this.a == decimal(\"1.500\")", msg));
            // Containers and membership follow the same equality.
            Assert.Equal(true, await Eval("[this.a] == [this.b]", msg));
            Assert.Equal(true, await Eval("{'k': this.a} == {'k': this.b}", msg));
            Assert.Equal(true, await Eval("this.a in [this.b]", msg));
            // Negative controls.
            Assert.Equal(false, await Eval("this.a == decimal(\"9\")", msg));
            Assert.Equal(false, await Eval("[this.a] == [decimal(\"9\")]", msg));
            Assert.Equal(false, await Eval("this.a in [decimal(\"9\")]", msg));
            // Decimal-free comparisons are unaffected.
            Assert.Equal(true, await Eval("[1, 2] == [1, 2]", msg));
            Assert.Equal(false, await Eval("[1, 2] == [2, 1]", msg));
            Assert.Equal(true, await Eval("2 in [1, 2]", msg));
            Assert.Equal(false, await Eval("3 in [1, 2]", msg));
        }

        /// <summary>
        ///     Cross-client parity: a bare <c>confluent.type.Decimal</c> field is usable with
        ///     <c>decimals.*</c>, <c>==</c>, <c>string()</c> and <c>double()</c> with <b>no
        ///     <c>decimal(...)</c> call</b> on it. The discriminating case is the scale-differing
        ///     equality: a client comparing decimals by their protobuf encoding (unscaled bytes
        ///     plus scale, field by field) answers false for <c>decimal("12.340")</c>, because
        ///     12.34 and 12.340 are the same number in two different encodings.
        /// </summary>
        [Fact]
        public async Task ProtoDecimalNeedsNoConstructor()
        {
            Confluent.SchemaRegistry.Serdes.Protobuf.Decimal dec = 12.34m.ToProtobufDecimal();

            // Bare: no constructor call on the field.
            Assert.Equal(true, await Eval("decimals.eq(this, decimal(\"12.34\"))", dec));
            Assert.Equal(true, await Eval("decimals.gt(this, decimal(\"10.00\"))", dec));
            // The wrapped form must keep working (decimal(...) re-entry).
            Assert.Equal(true, await Eval("decimals.eq(decimal(this), decimal(\"12.34\"))", dec));
            // `==` is numeric on it: 12.34 equals 12.340 despite the differing scale.
            Assert.Equal(true, await Eval("this == decimal(\"12.340\")", dec));
            Assert.Equal(false, await Eval("this != decimal(\"12.340\")", dec));
            Assert.Equal(true, await Eval("decimals.lt(this, decimal(\"100\"))", dec));
            // Negative control: a false comparison must still be false.
            Assert.Equal(false, await Eval("decimals.gt(this, decimal(\"100\"))", dec));
            Assert.Equal(true, await Eval("string(this) == \"12.34\"", dec));
            Assert.Equal(true, await Eval("double(this) == 12.34", dec));
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

        /// <summary>
        ///     Cross-client parity: an Avro <c>decimal</c> logical type is usable as a Decimal
        ///     with <b>no <c>decimal(...)</c> call</b>, and the wrapped form keeps working
        ///     alongside it. The Avro registry's value adapter carries the AvroDecimal as a
        ///     DecimalT (see CelExecutor.AvroValueToCel), so <c>decimals.*</c> accept it directly
        ///     and <c>==</c> stays numeric — without it, cel.net's own <c>avro.decimal</c> value
        ///     is a different CEL type and <c>==</c> against a decimal literal answers false.
        /// </summary>
        [Fact]
        public async Task AvroLogicalDecimalNeedsNoConstructor()
        {
            var schema = (RecordSchema)Avro.Schema.Parse(@"{
                ""type"": ""record"", ""name"": ""DecimalRecord"",
                ""fields"": [ { ""name"": ""amount"",
                    ""type"": { ""type"": ""bytes"", ""logicalType"": ""decimal"",
                                ""precision"": 8, ""scale"": 2 } } ] }");
            var record = new GenericRecord(schema);
            record.Add("amount", new AvroDecimal(12.34m));

            // Bare: no constructor call on the field.
            Assert.Equal(true, await Eval("decimals.eq(this.amount, decimal(\"12.34\"))", record));
            Assert.Equal(true, await Eval("decimals.gt(this.amount, decimal(\"10.00\"))", record));
            // The wrapped form must keep working (decimal(...) re-entry).
            Assert.Equal(true,
                await Eval("decimals.eq(decimal(this.amount), decimal(\"12.34\"))", record));
            // `==` is numeric on it: 12.34 equals 12.340 despite the differing scale.
            Assert.Equal(true, await Eval("this.amount == decimal(\"12.340\")", record));
            // The schema's scale is applied, not guessed: as scale 0 this would be 1234.
            Assert.Equal(true, await Eval("decimals.lt(this.amount, decimal(\"100\"))", record));
            // Negative control: a false comparison still fails.
            Assert.Equal(false, await Eval("decimals.gt(this.amount, decimal(\"100\"))", record));
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
            Assert.Equal(true, await Eval("timestamp(this.ts) < now", record));
        }

        /// <summary>
        ///     Cross-client parity: an Avro timestamp logical type is usable as a timestamp with
        ///     <b>no constructor call at all</b>. cel.net's TypeAdapterSupport maps a DateTime to
        ///     a CEL timestamp, so it is comparable against <c>now</c> and carries the timestamp
        ///     accessors. Every one of the seven clients has this test; the constructor is only
        ///     needed for a plain numeric field whose unit the schema cannot supply.
        /// </summary>
        [Fact]
        public async Task AvroLogicalTimestampNeedsNoConstructor()
        {
            var schema = (RecordSchema)Avro.Schema.Parse(@"{
                ""type"": ""record"", ""name"": ""TsRecord"",
                ""fields"": [ { ""name"": ""ts"",
                    ""type"": { ""type"": ""long"", ""logicalType"": ""timestamp-millis"" } } ] }");

            GenericRecord At(DateTime when)
            {
                var r = new GenericRecord(schema);
                r.Add("ts", when);
                return r;
            }

            // Bare comparison against `now`, plus the control that proves it really compares.
            Assert.Equal(true, await Eval("this.ts < now",
                At(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc))));
            Assert.Equal(false, await Eval("this.ts < now",
                At(new DateTime(2100, 1, 1, 0, 0, 0, DateTimeKind.Utc))));

            // The schema's millis unit is applied, not guessed, and the accessors work directly.
            var exact = At(new DateTime(2023, 11, 14, 22, 13, 20, 123, DateTimeKind.Utc));
            Assert.Equal(true, await Eval(
                "this.ts == timestamp(\"2023-11-14T22:13:20.123Z\")", exact));
            Assert.Equal(true, await Eval("this.ts.getFullYear() == 2023", exact));
        }

        /// <summary>
        ///     The two-argument timestamp constructor has to refuse an epoch outside the CEL
        ///     range, as the reference's <c>instantOfEpoch</c> does. Protobuf's own formatting
        ///     caught the upper end, but not the lower: measured before the check,
        ///     <c>timestamp(-62135596801, 0)</c> rendered as <c>0000-12-31T23:59:59Z</c>.
        /// </summary>
        [Theory]
        [InlineData("string(timestamp(0, 0))", "1970-01-01T00:00:00Z")]
        [InlineData("string(timestamp(253402300799, 0))", "9999-12-31T23:59:59Z")]
        [InlineData("string(timestamp(-62135596800, 0))", "0001-01-01T00:00:00Z")]
        // int64 nanoseconds cannot leave the range, so the widest nanos value still answers.
        [InlineData("string(timestamp(9223372036854775807, 9))", "2262-04-11T23:47:16.854775807Z")]
        // FloorDiv, not truncation: a pre-epoch value keeps a non-negative sub-second part.
        [InlineData("string(timestamp(-1500, 3))", "1969-12-31T23:59:58.500Z")]
        public async Task TwoArgTimestampInRange(string expr, string expected)
        {
            Assert.Equal(expected, await Eval(expr, "x"));
        }

        [Theory]
        [InlineData("string(timestamp(253402300800, 0))")]
        [InlineData("string(timestamp(-62135596801, 0))")]
        [InlineData("string(timestamp(9223372036854775807, 0))")]
        [InlineData("string(timestamp(-9223372036854775807, 0))")]
        [InlineData("string(timestamp(9223372036854775807, 3))")]
        public async Task TwoArgTimestampOutOfRangeIsRefused(string expr)
        {
            await Assert.ThrowsAnyAsync<Exception>(() => Eval(expr, "x"));
        }
    }
}