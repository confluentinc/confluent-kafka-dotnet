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
        public async Task VariantFunctions(string expr, bool expected)
        {
            Assert.Equal(expected, await Eval(expr, Doc));
        }

        [Fact]
        public async Task RejectsStringPassedToVariant()
        {
            // variant(string) is an error; strings must go through variants.parseJson.
            await Assert.ThrowsAnyAsync<System.Exception>(
                () => Eval("variants.type(variant(this)) == 'object'", Doc));
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
