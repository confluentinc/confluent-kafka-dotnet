using System;
using System.Threading.Tasks;
using Confluent.SchemaRegistry.Rules;
using Google.Protobuf;
using Xunit;
using PbDecimal = Confluent.SchemaRegistry.Serdes.Protobuf.Decimal;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     A <c>confluent.type.Decimal</c> bound straight to <c>this</c>, which is what a
    ///     <c>CEL_FIELD</c> rule on a decimal field does.
    /// </summary>
    /// <remarks>
    ///     The domain path picks its registry from the value, as the reference does. The value
    ///     therefore has to be sampled <i>before</i> the conversions that follow, and it was being
    ///     sampled between two of them: <c>ToCelValue</c> routed through
    ///     <c>ToCelDecimalOrNull</c>, so the decimal was already a <c>DecimalT</c> - not an
    ///     <c>IMessage</c> - and the JSON registry was chosen.
    /// </remarks>
    public class CelDirectDecimalRegistryTests
    {
        // 0x04D2 = 1234 unscaled, i.e. 12.34 at scale 2.
        private static PbDecimal Decimal1234() => new PbDecimal
        {
            Value = ByteString.CopyFrom(new byte[] { 0x04, 0xD2 }), Precision = 8, Scale = 2
        };

        private static Task<object> Eval(string expr) =>
            new CelValidator().Execute(new ValidationRule { Name = "r", Expr = expr }, null,
                Decimal1234());

        /// <summary>
        ///     The discriminator: naming the protobuf type only resolves if the protobuf
        ///     registry was selected. With the JSON registry this failed the check with
        ///     "undeclared reference to 'confluent' (in container '')".
        /// </summary>
        [Fact]
        public async Task TheProtobufTypeNameResolves()
        {
            Assert.Equal(true, await Eval("type(this) == confluent.type.Decimal"));
        }

        /// <summary>
        ///     ...and the decimal overloads still see a decimal, which is the half that worked
        ///     before and must not regress: the value is converted for binding even though the
        ///     registry is chosen from what arrived.
        /// </summary>
        [Theory]
        [InlineData("decimals.gt(this, decimal(\"1.00\"))")]
        [InlineData("this == decimal(\"12.34\")")]
        // Numeric equality, not the field-by-field protobuf one: 12.34 and 12.340 are equal.
        [InlineData("decimals.eq(this, decimal(\"12.340\"))")]
        [InlineData("string(this) == \"12.34\"")]
        [InlineData("double(this) == 12.34")]
        public async Task TheDecimalOverloadsStillApply(string expr)
        {
            Assert.Equal(true, await Eval(expr));
        }

        /// <summary>
        ///     Field selection on a directly bound decimal remains unsupported, pinned as known.
        ///     The reference allows it: it coerces inside the <c>decimals.*</c> functions rather
        ///     than at binding, so its <c>this</c> stays a message. Here the value is a
        ///     <c>DecimalT</c> by then, so the selection has no receiver - and now fails at
        ///     evaluation rather than at check time, which is the registry fix showing through.
        /// </summary>
        [Fact]
        public async Task FieldSelectionOnADirectlyBoundDecimalIsNotSupported()
        {
            RuleException e = await Assert.ThrowsAsync<RuleException>(
                () => Eval("this.scale == 2"));
            // Not the old check-time "failed resolution of 'confluent.type.Decimal'": the type
            // resolves now, and only the selection is unavailable.
            Assert.DoesNotContain("failed resolution", e.ToString());
        }
    }
}
