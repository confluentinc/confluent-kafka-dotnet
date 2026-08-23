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

using System.Threading.Tasks;
using Confluent.SchemaRegistry.Rules;
using Google.Protobuf;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     Tests for the CEL <c>variant(...)</c> / <c>variants.*</c> function family, and for
    ///     marshalling a Variant into CEL from the two schema-side shapes: an Avro variant
    ///     (the logical type decodes to a <see cref="Variant" />) and a Protobuf
    ///     <c>confluent.type.Variant</c> message.
    /// </summary>
    public class CelVariantTests
    {
        // A JSON document exercising objects, arrays, an explicit null, and nesting.
        private const string Doc =
            "{\"name\":\"alice\",\"age\":30,\"explicit\":null," +
            "\"nested\":{\"x\":1},\"scores\":[10,20,30]}";

        private static ValidationRule Rule(string expr) => new ValidationRule { Name = "r", Expr = expr };

        private static Task<object> Eval(string expr, object value) =>
            new CelValidator().Execute(Rule(expr), null, value);

        // ---- variants.* over a parsed JSON string bound as `this` ----

        [Theory]
        [InlineData("variants.type(variants.parseJson(this)) == 'object'", true)]
        [InlineData("variants.as(variants.field(variants.parseJson(this), 'name'), 'string') == 'alice'", true)]
        [InlineData("variants.as(variants.field(variants.parseJson(this), 'age'), 'int') == 30", true)]
        // A missing field is CEL null (absent); an explicit JSON null is a present variant-null.
        [InlineData("variants.field(variants.parseJson(this), 'missing') == null", true)]
        [InlineData("variants.isNull(variants.field(variants.parseJson(this), 'explicit'))", true)]
        [InlineData("!variants.isNull(variants.field(variants.parseJson(this), 'missing'))", true)]
        [InlineData("variants.as(variants.path(variants.parseJson(this), '$.nested.x'), 'int') == 1", true)]
        [InlineData("variants.as(variants.index(variants.field(variants.parseJson(this), 'scores'), 2), 'int') == 30", true)]
        // tryAs returns CEL null on a type mismatch (age is an int, not a string).
        [InlineData("variants.tryAs(variants.field(variants.parseJson(this), 'age'), 'string') == null", true)]
        [InlineData("variants.toJson(variants.field(variants.parseJson(this), 'nested')) == '{\"x\":1}'", true)]
        // variant(null) passes CEL null through as CEL null (no error), and navigation over it
        // is still CEL null - so a null argument (here a missing field) composes cleanly.
        [InlineData("dyn(variant(null)) == null", true)]
        [InlineData("variants.field(variant(variants.field(variants.parseJson(this), 'missing')), 'k') == null", true)]
        public async Task VariantFunctions(string expr, bool expected)
        {
            Assert.Equal(expected, await Eval(expr, Doc));
        }

        [Theory]
        // Empty/whitespace input is a soft failure: variants.tryParseJson catches the parse error
        // and yields CEL null, whereas variants.parseJson (strict) throws.
        [InlineData("")]
        [InlineData("   ")]
        [InlineData("\t\n")]
        public async Task TryParseJson_EmptyOrWhitespace_IsNull(string input)
        {
            Assert.Equal(true, await Eval("variants.tryParseJson(this) == null", input));
        }

        [Fact]
        public async Task ParseJson_EmptyInput_Throws()
        {
            // Strict parseJson surfaces the failure rather than returning null.
            await Assert.ThrowsAnyAsync<System.Exception>(
                () => Eval("variants.type(variants.parseJson(this)) == 'object'", ""));
        }

        [Fact]
        public async Task RejectsStringPassedToVariant()
        {
            // variant(string) is an error; strings must go through variants.parseJson.
            await Assert.ThrowsAnyAsync<System.Exception>(
                () => Eval("variants.type(variant(this)) == 'object'", Doc));
        }

        // ---- variants.as('timestamp') on NANOS variants: floor-divide + preserve nanos, match Java ----
        //
        // variants.as extracts a NANOS variant via TimestampUtils.FromEpochNanos, which keeps full
        // nanosecond precision in the proto Timestamp and floor-divides toward -infinity for
        // negatives - matching Java's variantGetTimestamp. Sub-second nanos are observable at the
        // CEL surface as of Cel.NET 2.1.1, which fixed TimestampT.TimestampOf(Timestamp) dropping
        // them (NodaTime Instant is immutable; the pre-2.1.1 code discarded PlusNanoseconds's
        // result). The nanos assertion below pins against an RFC-3339 string timestamp (a distinct,
        // always-correct parse path) so it fails if the proto-wrapping path ever drops nanos again.

        [Fact]
        public async Task VariantAsTimestamp_NanosPositive()
        {
            // 2_500_000_000 ns = 2.5 s -> seconds = 2, nanos = 500_000_000.
            var b = new VariantBuilder();
            b.AppendTimestampNanosTz(2_500_000_000L);
            Variant v = b.Build();

            Assert.Equal(true, await Eval(
                "variants.as(variant(this), 'timestamp') == timestamp(2500000000, 9)", v));
            // Independent nanos check via the RFC-3339 string path (would fail if nanos were dropped).
            Assert.Equal(true, await Eval(
                "variants.as(variant(this), 'timestamp') == timestamp('1970-01-01T00:00:02.500000000Z')", v));
        }

        [Fact]
        public async Task VariantAsTimestamp_NanosNegative_FloorDivides()
        {
            // -1_000_000_001 ns floor-divides to seconds=-2 (round toward -infinity). The old
            // code did raw/1000 = -1_000_000 us (truncating toward zero) then floorDiv by 1e6,
            // yielding seconds=-1 - an off-by-one second. Match Java's Math.floorDiv.
            var b = new VariantBuilder();
            b.AppendTimestampNanosNtz(-1_000_000_001L);
            Variant v = b.Build();

            Assert.Equal(true, await Eval(
                "variants.as(variant(this), 'timestamp') == timestamp(-1000000001, 9)", v));
        }

        // ---- Marshalling: the two schema-side shapes into CEL ----

        [Fact]
        public async Task AvroVariantIntoCel()
        {
            // An Avro variant field decodes (via VariantLogicalType) to a Variant; a rule
            // binds that as `this`, and variant(dyn) accepts it directly.
            Variant v = Variant.ParseJson(Doc);
            Assert.Equal(true, await Eval(
                "variants.as(variants.field(variant(this), 'name'), 'string') == 'alice'", v));
        }

        [Fact]
        public async Task ProtoConfluentTypeVariantIntoCel()
        {
            // A confluent.type.Variant proto message bound as `this`; variant(dyn) unwraps it.
            Variant v = Variant.ParseJson(Doc);
            var msg = new Confluent.SchemaRegistry.Serdes.Protobuf.Variant
            {
                Metadata = ByteString.CopyFrom(v.MetadataBytes),
                Value = ByteString.CopyFrom(v.ValueBytes)
            };
            Assert.Equal(true, await Eval(
                "variants.as(variants.field(variant(this), 'age'), 'int') == 30", msg));
        }
    }
}
