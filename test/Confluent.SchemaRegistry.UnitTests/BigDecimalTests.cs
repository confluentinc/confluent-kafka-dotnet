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

using System;
using System.Collections.Generic;
using System.Numerics;
using Xunit;

namespace Confluent.SchemaRegistry.UnitTests
{
    /// <summary>Tests for the hand-written <see cref="BigDecimal" /> value type.</summary>
    public class BigDecimalTests
    {
        // A whole-number double/float keeps a trailing ".0" (scale 1), matching Java
        // BigDecimal.valueOf(double) / Float.toString and Python str(float) - not .NET's "R"
        // which would drop it ("2", scale 0).
        [Fact]
        public void FromDouble_WholeNumber_KeepsTrailingZero()
        {
            Assert.Equal("2.0", BigDecimal.FromDouble(2.0).ToPlainString());
            Assert.Equal(1, BigDecimal.FromDouble(2.0).Scale);
            Assert.Equal("-5.0", BigDecimal.FromDouble(-5.0).ToPlainString());
            // Fractional and scientific values are unaffected.
            Assert.Equal("0.1", BigDecimal.FromDouble(0.1).ToPlainString());
            Assert.Equal("2.5", BigDecimal.FromDouble(2.5).ToPlainString());
        }

        [Fact]
        public void FromFloat_WholeNumber_KeepsTrailingZero()
        {
            Assert.Equal("2.0", BigDecimal.FromFloat(2.0f).ToPlainString());
            Assert.Equal(1, BigDecimal.FromFloat(2.0f).Scale);
            Assert.Equal("0.1", BigDecimal.FromFloat(0.1f).ToPlainString());
        }

        // The whole-number scale carries through multiply (1 + 1 -> 2): decimal(2.0) * decimal(0.5).
        [Fact]
        public void FromDouble_WholeNumberScale_PropagatesThroughMultiply()
        {
            var product = BigDecimal.FromDouble(2.0).Multiply(BigDecimal.FromDouble(0.5));
            Assert.Equal("1.00", product.ToPlainString());
        }

        // Equals is value equality (scale-independent), so equal values must hash equally -
        // including zero at different scales.
        [Fact]
        public void EqualsAndHashCode_ZeroWithScale_AreConsistent()
        {
            BigDecimal zero0 = BigDecimal.Zero;
            BigDecimal zero2 = BigDecimal.Parse("0.00");
            Assert.True(zero0.Equals(zero2));
            Assert.Equal(zero0.GetHashCode(), zero2.GetHashCode());
        }

        [Fact]
        public void EqualsAndHashCode_NonZeroTrailingZeros_AreConsistent()
        {
            BigDecimal a = BigDecimal.Parse("1.50");
            BigDecimal b = BigDecimal.Parse("1.5");
            Assert.True(a.Equals(b));
            Assert.Equal(a.GetHashCode(), b.GetHashCode());
        }

        // FromDecimal is lossless: it preserves scale/trailing zeros (1.50m -> scale 2, "1.50").
        [Fact]
        public void FromDecimal_PreservesScaleAndTrailingZeros()
        {
            BigDecimal d = BigDecimal.FromDecimal(1.50m);
            Assert.Equal("1.50", d.ToPlainString());
            Assert.Equal(2, d.Scale);
        }

        [Fact]
        public void FromDecimal_Negative_RoundTripsThroughToDecimal()
        {
            Assert.Equal(-123.456m, BigDecimal.FromDecimal(-123.456m).ToDecimal());
        }

        [Fact]
        public void FromDecimal_ToDecimal_RoundTrips()
        {
            decimal[] values =
            {
                0m,
                1m,
                1.50m,
                -123.456m,
                12.34m,
                123456789123456789.56m,
                -4.1748330066797328106875724500m,
                decimal.MaxValue,
                decimal.MinValue
            };

            foreach (decimal value in values)
            {
                Assert.Equal(value, BigDecimal.FromDecimal(value).ToDecimal());
            }
        }

        // A BigDecimal whose integer part exceeds System.Decimal's range overflows on ToDecimal.
        [Fact]
        public void ToDecimal_ValueTooLarge_ThrowsOverflow()
        {
            BigDecimal tooBig = BigDecimal.Parse("123456789012345678901234567890");
            Assert.Throws<OverflowException>(() => tooBig.ToDecimal());
        }

        // 1.5e30 at scale 30 equals exactly 1.5; it must convert rather than overflow the 10^30 divisor.
        [Fact]
        public void ToDecimal_ExactlyRepresentableHighScale_Converts()
        {
            BigDecimal d = new BigDecimal(BigInteger.Parse("1500000000000000000000000000000"), 30);
            Assert.Equal(1.5m, d.ToDecimal());
        }

        // Values below 1e-28 (System.Decimal's finest granularity) round to 0.
        [Fact]
        public void ToDecimal_SubGranularity_RoundsToZero()
        {
            Assert.Equal(0m, new BigDecimal(BigInteger.One, 30).ToDecimal());
            Assert.Equal(0m, new BigDecimal(BigInteger.One, 29).ToDecimal());
        }

        // HALF_UP at the 28-dp boundary: 5e-29 rounds up to 1e-28.
        [Fact]
        public void ToDecimal_HalfUpAtBoundary_RoundsUp()
        {
            BigDecimal d = new BigDecimal(new BigInteger(5), 29);
            Assert.Equal(0.0000000000000000000000000001m, d.ToDecimal());
        }

        // A genuinely too-large integer value still overflows.
        [Fact]
        public void ToDecimal_TooLargeInteger_ThrowsOverflow()
        {
            BigDecimal d = new BigDecimal(BigInteger.Parse("123456789012345678901234567890"), 0);
            Assert.Throws<OverflowException>(() => d.ToDecimal());
        }

        // ---- width ceiling -----------------------------------------------------------
        //
        // BigInteger here is unbounded until the process dies, where Java raises an
        // ArithmeticException at 646456993 digits. The ceiling stands in for that, as a bound
        // rather than as a model of BigDecimal's domain - so it is deliberately tighter than
        // the JVM's, and the split below is this client's.
        //
        // The dividing line is not arithmetic vs. rescale, it is whether the operation has to
        // build a positional form. Add/Subtract/Remainder align their operands through
        // Rescale, which materialises a power of ten; Multiply does not, and neither does
        // comparison, negation or Abs. Measured on the shared libmpdec in the Python client,
        // peak RSS on operands 1e2147483647 and 3: mul, div, comparison, neg and abs all
        // 13 MB; add 1738 MB, sub 1738 MB, remainder 1733 MB.

        [Fact]
        public void Add_AligningTwoDistantScales_IsRefused()
        {
            var wide = new BigDecimal(BigInteger.One, -2000000000);   // 1e2000000000
            var one = new BigDecimal(BigInteger.One, 0);
            Assert.Throws<ArithmeticException>(() => wide.Add(one));
            Assert.Throws<ArithmeticException>(() => wide.Subtract(one));
            Assert.Throws<ArithmeticException>(() => wide.Remainder(one));
            var tiny = new BigDecimal(BigInteger.One, 2000000000);    // 1e-2000000000
            Assert.Throws<ArithmeticException>(() => tiny.Add(one));
            Assert.Throws<ArithmeticException>(() => wide.Add(tiny));
        }

        // Remainder is the one operation where this client's bound is *tighter* than the
        // reference's, and it is worth pinning rather than discovering. Java computes
        // `this.subtract(this.divideToIntegralValue(divisor).multiply(divisor))`, so its cost
        // follows the integral quotient - `1e-2147483647 mod 1e2147483647` is the dividend
        // itself at precision 1, scale 2147483647, which the JVM returns and libmpdec produces
        // in microseconds. This implementation instead aligns both operands through Rescale,
        // so that same call really would build a 4.3-billion-digit frame and is refused. The
        // guard is correct for the code as written; the divergence is the code, not the guard.
        [Fact]
        public void Remainder_IsBoundedByAlignment_NotByTheQuotient()
        {
            var tiny = new BigDecimal(BigInteger.One, 2000000000);   // 1e-2000000000
            var wide = new BigDecimal(BigInteger.One, -2000000000);  // 1e2000000000
            // Java accepts this and returns the dividend; this client refuses it.
            Assert.Throws<ArithmeticException>(() => tiny.Remainder(wide));
            // Operands whose magnitudes are close stay narrow and are accepted, as everywhere.
            Assert.Equal(0, wide.Remainder(wide).Signum);
            Assert.Equal("1", new BigDecimal(BigInteger.Parse("10000000000000000000000000000000000000000"), 0)
                .Remainder(new BigDecimal(new BigInteger(3), 0)).ToPlainString());
        }

        // Expanding a *zero* is free, so the frame is set by the operands that actually have
        // digits - which means several of these turn on which operand expands, not on how far
        // apart the scales are. Every row measured on the JDK:
        //   0E+2e9 + 0E-2e9  -> precision 1          (both zero)
        //   0E+2e9 + 1       -> precision 1, scale 0 (aligns to 0, so the *zero* expands)
        //   1 + 0E-2e9       -> ArithmeticException  (aligns to 2e9, so the *one* expands)
        //   0E+2e9 mod 1E-2e9 -> precision 1
        [Fact]
        public void ExpandingAZeroOperandIsFree()
        {
            var zeroCoarse = new BigDecimal(BigInteger.Zero, -2000000000); // 0E+2e9
            var zeroFine = new BigDecimal(BigInteger.Zero, 2000000000);    // 0E-2e9
            var one = new BigDecimal(BigInteger.One, 0);

            Assert.Equal(0, zeroCoarse.Add(zeroFine).Signum);
            Assert.Equal(0, zeroCoarse.Subtract(zeroFine).Signum);
            Assert.Equal("1", zeroCoarse.Add(one).ToPlainString());
            Assert.Equal(0, zeroCoarse.Remainder(new BigDecimal(BigInteger.One, 2000000000)).Signum);

            // The row that must still be refused: here the *one* is what expands.
            Assert.Throws<ArithmeticException>(() => one.Add(zeroFine));
        }

        // The must-fail twin: Multiply does not align, so it is unbounded at any width, and
        // alignment that stays narrow is fine however extreme both operands are.
        [Fact]
        public void Multiply_AndNarrowAlignment_StayUnbounded()
        {
            var wide = new BigDecimal(BigInteger.One, -2000000000);
            var tiny = new BigDecimal(BigInteger.One, 2000000000);
            Assert.Equal(0, wide.Multiply(tiny).CompareTo(new BigDecimal(BigInteger.One, 0)));
            // Comparison short-circuits on sign and then on adjusted exponent, so it never
            // aligns for operands this far apart - it used to, and cost as much as Add.
            Assert.True(tiny.CompareTo(wide) < 0);
            Assert.True(wide.CompareTo(tiny) > 0);
            Assert.True(tiny.Negate().CompareTo(wide) < 0);
            Assert.True(wide.Negate().CompareTo(tiny.Negate()) < 0);
            Assert.False(tiny.Equals(wide));
            Assert.Equal(0, wide.Subtract(wide).Signum);
            Assert.Equal("13.84",
                new BigDecimal(new BigInteger(1234), 2)
                    .Add(new BigDecimal(new BigInteger(15), 1)).ToPlainString());
        }

        // SetScale pays for Pow10 whichever way the scale moves - multiplier when expanding,
        // divisor when coarsening - so unlike libmpdec's rescale both directions are bounded
        // here. Zero is not exempt for the same reason: the power of ten is built regardless.
        [Fact]
        public void SetScale_PastTheCeiling_IsRefused()
        {
            var d = new BigDecimal(new BigInteger(123), 2);
            Assert.Throws<ArithmeticException>(() => d.SetScale(100000000, BigDecimal.Rounding.HalfUp));
            Assert.Throws<ArithmeticException>(() => d.SetScale(-100000000, BigDecimal.Rounding.HalfUp));
            Assert.Throws<ArithmeticException>(() => d.SetScale(2147483647, BigDecimal.Rounding.Down));
            // Zero is exempt, and this line asserted the opposite until it was measured: the
            // reference moves a zero to any scale exactly - `new BigDecimal(BigInteger.ZERO,
            // 2147483647)` is precision 1 - and SetScale now short-circuits rather than
            // building a Pow10 to multiply by zero.
            Assert.Equal(2147483647, BigDecimal.Zero.SetScale(2147483647, BigDecimal.Rounding.Floor).Scale);
            Assert.Equal(0, BigDecimal.Zero.SetScale(2147483647, BigDecimal.Rounding.Floor).Signum);
            Assert.Equal(int.MinValue,
                BigDecimal.Zero.SetScale(int.MinValue, BigDecimal.Rounding.Down).Scale);
        }

        [Fact]
        public void SetScale_WithinTheCeiling_StillAnswers()
        {
            var d = new BigDecimal(new BigInteger(123), 2);
            Assert.Equal("1", d.SetScale(0, BigDecimal.Rounding.HalfUp).ToPlainString());
            Assert.Equal("1.230", d.SetScale(3, BigDecimal.Rounding.HalfUp).ToPlainString());
            // "1." followed by 4000 fractional digits.
            Assert.Equal(4002, d.SetScale(4000, BigDecimal.Rounding.HalfUp).ToPlainString().Length);
        }

        // Multiplication adds the scales and division subtracts them, both in `int`, which
        // wraps: int.MaxValue + 1 is int.MinValue, so a vanishingly small number silently
        // became an enormous one. The reference refuses these - measured on the JDK:
        //   new BigDecimal(TEN, Integer.MAX_VALUE).multiply(new BigDecimal(TEN, 1)) -> Underflow
        //   new BigDecimal(TEN, Integer.MIN_VALUE) squared                           -> Overflow
        //   divide at MAX_VALUE by one at MIN_VALUE                                  -> Underflow
        [Fact]
        public void Multiply_ScaleOutOfIntRange_IsRefused()
        {
            var maxScale = new BigDecimal(new BigInteger(10), int.MaxValue);
            var minScale = new BigDecimal(new BigInteger(10), int.MinValue);
            var one = new BigDecimal(new BigInteger(10), 1);

            Assert.Throws<ArithmeticException>(() => maxScale.Multiply(one));
            Assert.Throws<ArithmeticException>(() => maxScale.Multiply(maxScale));
            Assert.Throws<ArithmeticException>(() => minScale.Multiply(minScale));
            Assert.Throws<ArithmeticException>(() => maxScale.Divide(minScale));

            // The must-fail twin: a scale sum that fits is unaffected.
            Assert.Equal(4, new BigDecimal(new BigInteger(15), 2)
                .Multiply(new BigDecimal(new BigInteger(15), 2)).Scale);
            Assert.Equal("2.25", new BigDecimal(new BigInteger(15), 1)
                .Multiply(new BigDecimal(new BigInteger(15), 1)).ToPlainString());
        }

        // Equality here is *numeric* (CompareTo == 0) to match `decimals.eq`, and that makes the
        // hash an obligation: two representations of one number must hash alike. Normalisation
        // stopped at scale zero, so (1, -1) and (10, 0) - both 10, and Equals says so - hashed
        // differently, and one went missing from any dictionary or set keyed on the other.
        // Java does not face this: its BigDecimal.equals compares unscaled *and* scale.
        [Fact]
        public void GetHashCode_AgreesWithNumericEquality_AtNegativeScales()
        {
            var a = new BigDecimal(BigInteger.One, -1);            // 1e1  = 10
            var b = new BigDecimal(new BigInteger(10), 0);         // 10
            var c = new BigDecimal(new BigInteger(1000), 2);       // 10.00

            Assert.True(a.Equals(b));
            Assert.Equal(a.GetHashCode(), b.GetHashCode());
            Assert.True(a.Equals(c));
            Assert.Equal(a.GetHashCode(), c.GetHashCode());

            // The property that was actually broken, stated as the caller sees it.
            var set = new HashSet<BigDecimal> { a };
            Assert.Contains(b, set);
            Assert.Contains(c, set);

            // Further out, where more than one strip is needed: 100 as (1,-2) and (100,0).
            var d = new BigDecimal(BigInteger.One, -2);
            var e = new BigDecimal(new BigInteger(100), 0);
            Assert.True(d.Equals(e));
            Assert.Equal(d.GetHashCode(), e.GetHashCode());

            // Every zero is value-equal whatever its scale, which already held.
            Assert.Equal(new BigDecimal(BigInteger.Zero, 0).GetHashCode(),
                new BigDecimal(BigInteger.Zero, 7).GetHashCode());
            Assert.Equal(new BigDecimal(BigInteger.Zero, 0).GetHashCode(),
                new BigDecimal(BigInteger.Zero, -7).GetHashCode());

            // And unequal values still differ, so the normalisation has not collapsed them.
            Assert.NotEqual(a.GetHashCode(), new BigDecimal(BigInteger.One, 0).GetHashCode());
        }

        // The coefficient-width guard now lives in one place and every write path goes through
        // it, so a decimal a rule computed and one the serde converts are accepted or refused
        // alike. The message-level CEL writer had its own unguarded copy of this count.
        [Fact]
        public void UnscaledPrecision_IsGuardedAndCountsDigits()
        {
            Assert.Equal(1u, BigDecimal.UnscaledPrecision(BigInteger.Zero));
            Assert.Equal(1u, BigDecimal.UnscaledPrecision(BigInteger.One));
            Assert.Equal(4u, BigDecimal.UnscaledPrecision(new BigInteger(1234)));
            Assert.Equal(4u, BigDecimal.UnscaledPrecision(new BigInteger(-1234)));
            Assert.Equal(38u, BigDecimal.UnscaledPrecision(BigInteger.Parse(new string('9', 38))));
            // Just inside the ceiling, then past it.
            Assert.Equal(4000u, BigDecimal.UnscaledPrecision(BigInteger.Parse(new string('9', 4000))));
            Assert.Throws<ArithmeticException>(
                () => BigDecimal.UnscaledPrecision(BigInteger.Parse(new string('9', 20000))));
        }

        // Remainder must be exact at any width - the Rust client computed it through a division
        // capped at 100 digits and silently returned a wrong residual past that. This one is
        // BigInteger `%` on the aligned coefficients, so it is exact by construction; measured
        // on the JDK, 10^k mod 3 is 1 for every k.
        [Fact]
        public void Remainder_IsExactPastAHundredDigits()
        {
            var three = new BigDecimal(new BigInteger(3), 0);
            foreach (int k in new[] { 99, 100, 101, 200, 1000 })
            {
                var v = new BigDecimal(BigInteger.Pow(10, k), 0);
                Assert.Equal("1", v.Remainder(three).ToPlainString());
            }
            // And a case whose answer is not 1: 10^200 mod 7 is 2 (10^6 = 1 mod 7, 200 mod 6 = 2).
            Assert.Equal("2", new BigDecimal(BigInteger.Pow(10, 200), 0)
                .Remainder(new BigDecimal(new BigInteger(7), 0)).ToPlainString());
        }

        // Rendering is a third site, reachable with no rescale at all: Divide holds its
        // coefficient to 38 digits while the scale runs free, so the value is cheap to hold
        // and enormous to print. The plain form pays for the scale in both directions.
        [Fact]
        public void ToPlainString_PastTheCeiling_IsRefused()
        {
            Assert.Throws<ArithmeticException>(
                () => new BigDecimal(BigInteger.One, 2000000000).ToPlainString());
            Assert.Throws<ArithmeticException>(
                () => new BigDecimal(BigInteger.One, -2000000000).ToPlainString());
            // No zero shortcut: a zero at an extreme scale renders as that many zeros.
            Assert.Throws<ArithmeticException>(
                () => new BigDecimal(BigInteger.Zero, -2000000000).ToPlainString());
            // Still renders below the ceiling, coefficient and scale independently.
            Assert.Equal("12.34", new BigDecimal(new BigInteger(1234), 2).ToPlainString());
            Assert.Equal(1000002, new BigDecimal(BigInteger.One, 1000000).ToPlainString().Length);
        }

        /// <summary>
        ///     The parsed scale is <c>fractionDigits - exponent</c>, which overflows int for an
        ///     extreme exponent - and unchecked int arithmetic wraps rather than throwing.
        ///     Measured, "1e-2147483648" gave scale -2147483648, turning a vanishingly small
        ///     value into an enormous one. The reference refuses it outright:
        ///     <c>new BigDecimal("1e-2147483648")</c> raises "Scale out of range."
        /// </summary>
        [Theory]
        [InlineData("1e-2147483648")]
        [InlineData("0.1e-2147483648")]
        public void ParseRefusesAScaleOutsideTheIntRange(string s)
        {
            Assert.Throws<FormatException>(() => BigDecimal.Parse(s));
        }

        /// <summary>The int32 boundaries themselves still parse.</summary>
        [Theory]
        [InlineData("1e-2147483647", 2147483647)]
        [InlineData("1e2147483647", -2147483647)]
        public void ParseAcceptsTheIntBoundaries(string s, int expectedScale)
        {
            Assert.Equal(expectedScale, BigDecimal.Parse(s).Scale);
        }
    }
}
