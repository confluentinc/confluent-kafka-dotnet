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
    }
}
