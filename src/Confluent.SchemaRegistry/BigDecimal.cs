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
using System.Globalization;
using System.Numerics;
using System.Text;

namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     Arbitrary-precision signed decimal, the CEL Decimal backing type for this client and
    ///     the unscaled/scale representation used by the Variant codec.
    ///     A value is <c>unscaled × 10^(-scale)</c> — a <see cref="BigInteger" /> unscaled
    ///     value and an <see cref="int" /> scale, exactly like Java's
    ///     <c>java.math.BigDecimal</c>. .NET's <see cref="decimal" /> tops out at 28–29
    ///     significant digits; parity with the other Schema Registry clients (Java/Go/Python/
    ///     JS/Rust) requires the 38-significant-digit division and square root those clients
    ///     produce, so the arithmetic here mirrors Java's <c>MathContext(38, HALF_UP)</c>.
    /// </summary>
    public readonly struct BigDecimal : IComparable<BigDecimal>, IEquatable<BigDecimal>
    {
        /// <summary>
        ///     Significant-digit precision for division and square root, matching Java's
        ///     <c>MathContext(38, HALF_UP)</c> used across the other clients.
        /// </summary>
        private const int DivisionPrecision = 38;

        private readonly BigInteger unscaled;
        private readonly int scale;

        public BigDecimal(BigInteger unscaled, int scale)
        {
            this.unscaled = unscaled;
            this.scale = scale;
        }

        public static readonly BigDecimal Zero = new BigDecimal(BigInteger.Zero, 0);

        /// <summary>Number of fractional digits (negative means trailing integer zeros).</summary>
        public int Scale => scale;

        /// <summary>Unscaled two's-complement integer value.</summary>
        public BigInteger Unscaled => unscaled;

        /// <summary>-1, 0 or 1 as the value is negative, zero or positive.</summary>
        public int Signum => unscaled.Sign;

        // ---- Construction ------------------------------------------------------------

        public static BigDecimal FromLong(long value) => new BigDecimal(new BigInteger(value), 0);

        public static BigDecimal FromBigInteger(BigInteger value) => new BigDecimal(value, 0);

        /// <summary>
        ///     Parse a decimal string, accepting an optional sign, an optional fractional
        ///     part, and optional scientific-notation exponent — the same grammar as Java's
        ///     <c>new BigDecimal(String)</c>.
        /// </summary>
        public static BigDecimal Parse(string s)
        {
            if (s == null)
            {
                throw new ArgumentNullException(nameof(s));
            }

            string str = s.Trim();
            if (str.Length == 0)
            {
                throw new FormatException("Empty string cannot be parsed as a decimal");
            }

            int exponent = 0;
            int eIndex = str.IndexOfAny(new[] { 'e', 'E' });
            if (eIndex >= 0)
            {
                exponent = int.Parse(str.Substring(eIndex + 1), CultureInfo.InvariantCulture);
                str = str.Substring(0, eIndex);
            }

            bool negative = false;
            if (str.Length > 0 && (str[0] == '+' || str[0] == '-'))
            {
                negative = str[0] == '-';
                str = str.Substring(1);
            }

            int fractionDigits = 0;
            int dotIndex = str.IndexOf('.');
            if (dotIndex >= 0)
            {
                fractionDigits = str.Length - dotIndex - 1;
                str = str.Substring(0, dotIndex) + str.Substring(dotIndex + 1);
            }

            if (str.Length == 0)
            {
                throw new FormatException($"'{s}' cannot be parsed as a decimal");
            }

            BigInteger digits = str.Length == 0 ? BigInteger.Zero : BigInteger.Parse(str, CultureInfo.InvariantCulture);
            if (negative)
            {
                digits = -digits;
            }

            // value = digits × 10^-fractionDigits × 10^exponent = digits × 10^-(fractionDigits - exponent)
            return new BigDecimal(digits, fractionDigits - exponent);
        }

        /// <summary>
        ///     Convert a <see cref="double" /> the way Java's <c>BigDecimal.valueOf(double)</c>
        ///     does: via the shortest decimal string that round-trips to the same double, not
        ///     the exact binary value. .NET's round-trip ("R") format is that shortest string.
        /// </summary>
        public static BigDecimal FromDouble(double value)
        {
            if (double.IsNaN(value) || double.IsInfinity(value))
            {
                throw new ArgumentException($"Cannot convert {value} to Decimal");
            }

            return Parse(value.ToString("R", CultureInfo.InvariantCulture));
        }

        // ---- Arithmetic --------------------------------------------------------------

        public BigDecimal Add(BigDecimal other)
        {
            int s = Math.Max(scale, other.scale);
            return new BigDecimal(Rescale(unscaled, scale, s) + Rescale(other.unscaled, other.scale, s), s);
        }

        public BigDecimal Subtract(BigDecimal other)
        {
            int s = Math.Max(scale, other.scale);
            return new BigDecimal(Rescale(unscaled, scale, s) - Rescale(other.unscaled, other.scale, s), s);
        }

        public BigDecimal Multiply(BigDecimal other)
        {
            return new BigDecimal(unscaled * other.unscaled, scale + other.scale);
        }

        public BigDecimal Negate() => new BigDecimal(-unscaled, scale);

        public BigDecimal Abs() => new BigDecimal(BigInteger.Abs(unscaled), scale);

        public BigDecimal Max(BigDecimal other) => CompareTo(other) >= 0 ? this : other;

        public BigDecimal Min(BigDecimal other) => CompareTo(other) <= 0 ? this : other;

        /// <summary>
        ///     Division at <see cref="DivisionPrecision" /> significant digits with HALF_UP
        ///     rounding — Java <c>divide(divisor, MathContext(38, HALF_UP))</c>. A result that
        ///     terminates within the precision keeps its exact (trailing-zero-stripped) value;
        ///     a non-terminating result is rounded to 38 significant digits.
        /// </summary>
        public BigDecimal Divide(BigDecimal divisor)
        {
            if (divisor.unscaled.IsZero)
            {
                throw new DivideByZeroException("division by zero");
            }

            if (unscaled.IsZero)
            {
                return Zero;
            }

            int sign = unscaled.Sign * divisor.unscaled.Sign;
            BigInteger a = BigInteger.Abs(unscaled);
            BigInteger b = BigInteger.Abs(divisor.unscaled);

            // The pure positive quotient P = a / b; the real value is P × 10^baseShift.
            int baseShift = divisor.scale - scale;

            // e = floor(log10(P)); P is in [10^(dA-dB-1), 10^(dA-dB+1)).
            int g = Digits(a) - Digits(b);
            bool ge = g >= 0 ? a >= b * Pow10(g) : a * Pow10(-g) >= b;
            int e = ge ? g : g - 1;

            // Scale so the rounded quotient carries exactly DivisionPrecision digits.
            int s = DivisionPrecision - 1 - e;
            BigInteger num = s >= 0 ? a * Pow10(s) : a;
            BigInteger den = s >= 0 ? b : b * Pow10(-s);
            BigInteger q = BigInteger.DivRem(num, den, out BigInteger rem);
            bool exact = rem.IsZero;
            if (2 * rem >= den)
            {
                q += 1;
            }

            // A rounding carry (…999 → 1000…) adds a digit; drop it and the now-trailing zero.
            if (Digits(q) > DivisionPrecision)
            {
                q /= 10;
                s -= 1;
            }

            if (exact)
            {
                StripTrailingZeros(ref q, ref s);
            }

            BigInteger resultUnscaled = sign < 0 ? -q : q;
            return new BigDecimal(resultUnscaled, s - baseShift);
        }

        /// <summary>
        ///     Remainder with the sign of the dividend — Java <c>BigDecimal.remainder</c>,
        ///     matching SQL MOD. Throws on a zero divisor.
        /// </summary>
        public BigDecimal Remainder(BigDecimal divisor)
        {
            if (divisor.unscaled.IsZero)
            {
                throw new DivideByZeroException("division by zero");
            }

            int s = Math.Max(scale, divisor.scale);
            BigInteger a = Rescale(unscaled, scale, s);
            BigInteger b = Rescale(divisor.unscaled, divisor.scale, s);
            // BigInteger division truncates toward zero, so the remainder takes the sign of a.
            BigInteger rem = a % b;
            return new BigDecimal(rem, s);
        }

        /// <summary>
        ///     Square root at <see cref="DivisionPrecision" /> significant digits, HALF_UP —
        ///     Java <c>sqrt(MathContext(38, HALF_UP))</c>. Throws on a negative value.
        /// </summary>
        public BigDecimal Sqrt()
        {
            if (unscaled.Sign < 0)
            {
                throw new ArithmeticException("square root of negative number");
            }

            if (unscaled.IsZero)
            {
                return Zero;
            }

            BigInteger u = unscaled;
            // eV = floor(log10(value)); eR ≈ floor(eV / 2) is the msd exponent of the root.
            int eV = (Digits(u) - 1) - scale;
            int eR = (int)Math.Floor(eV / 2.0);
            int s = DivisionPrecision - 1 - eR;

            // M = value × 10^(2s) must be an integer, i.e. 2s - scale >= 0.
            int exp2 = 2 * s - scale;
            if (exp2 < 0)
            {
                int bump = (-exp2 + 1) / 2;
                s += bump;
                exp2 = 2 * s - scale;
            }

            BigInteger m = u * Pow10(exp2);
            BigInteger q = ISqrt(m);
            bool exact = m == q * q;
            // HALF_UP: round up when m >= (q + 0.5)^2, i.e. 4m >= (2q + 1)^2.
            if (4 * m >= (2 * q + 1) * (2 * q + 1))
            {
                q += 1;
            }

            if (Digits(q) > DivisionPrecision)
            {
                q /= 10;
                s -= 1;
            }

            if (exact)
            {
                StripTrailingZeros(ref q, ref s);
            }

            return new BigDecimal(q, s);
        }

        // ---- Rounding ----------------------------------------------------------------

        public enum Rounding
        {
            HalfUp,
            Down,
            Floor,
            Ceiling
        }

        /// <summary>
        ///     Return the value at <paramref name="newScale" /> fractional digits, rounding
        ///     with <paramref name="mode" /> — Java <c>setScale(newScale, mode)</c>.
        /// </summary>
        public BigDecimal SetScale(int newScale, Rounding mode)
        {
            if (newScale >= scale)
            {
                return new BigDecimal(unscaled * Pow10(newScale - scale), newScale);
            }

            BigInteger divisor = Pow10(scale - newScale);
            BigInteger q = BigInteger.DivRem(unscaled, divisor, out BigInteger rem);
            if (!rem.IsZero)
            {
                switch (mode)
                {
                    case Rounding.HalfUp:
                        if (2 * BigInteger.Abs(rem) >= divisor)
                        {
                            q += unscaled.Sign;
                        }

                        break;
                    case Rounding.Down:
                        break;
                    case Rounding.Floor:
                        if (unscaled.Sign < 0)
                        {
                            q -= 1;
                        }

                        break;
                    case Rounding.Ceiling:
                        if (unscaled.Sign > 0)
                        {
                            q += 1;
                        }

                        break;
                }
            }

            return new BigDecimal(q, newScale);
        }

        // ---- Comparison / equality ---------------------------------------------------

        public int CompareTo(BigDecimal other)
        {
            int s = Math.Max(scale, other.scale);
            return Rescale(unscaled, scale, s).CompareTo(Rescale(other.unscaled, other.scale, s));
        }

        /// <summary>Numeric equality (ignores scale), matching <c>decimals.eq</c>.</summary>
        public bool Equals(BigDecimal other) => CompareTo(other) == 0;

        public override bool Equals(object obj) => obj is BigDecimal other && Equals(other);

        public override int GetHashCode()
        {
            // Hash on the trailing-zero-stripped form so equal values hash equally.
            BigInteger q = unscaled;
            int sc = scale;
            StripTrailingZeros(ref q, ref sc);
            return q.GetHashCode() * 397 ^ sc;
        }

        // ---- Conversion --------------------------------------------------------------

        /// <summary>
        ///     Plain decimal string with no scientific notation — Java <c>toPlainString</c>.
        /// </summary>
        public string ToPlainString()
        {
            bool negative = unscaled.Sign < 0;
            string digits = BigInteger.Abs(unscaled).ToString(CultureInfo.InvariantCulture);

            var sb = new StringBuilder();
            if (scale <= 0)
            {
                // Integer value; append -scale trailing zeros.
                sb.Append(digits);
                sb.Append('0', -scale);
            }
            else if (digits.Length > scale)
            {
                int pointPos = digits.Length - scale;
                sb.Append(digits, 0, pointPos);
                sb.Append('.');
                sb.Append(digits, pointPos, scale);
            }
            else
            {
                // 0.00…digits
                sb.Append("0.");
                sb.Append('0', scale - digits.Length);
                sb.Append(digits);
            }

            return (negative ? "-" : string.Empty) + sb;
        }

        /// <summary>
        ///     Nearest <see cref="double" /> (may lose precision; ±Infinity out of range) —
        ///     Java <c>doubleValue</c>.
        /// </summary>
        public double ToDouble()
        {
            try
            {
                return double.Parse(ToPlainString(), NumberStyles.Float, CultureInfo.InvariantCulture);
            }
            catch (OverflowException)
            {
                return unscaled.Sign < 0 ? double.NegativeInfinity : double.PositiveInfinity;
            }
        }

        public override string ToString() => ToPlainString();

        // ---- Helpers -----------------------------------------------------------------

        private static BigInteger Pow10(int n) => BigInteger.Pow(10, n);

        private static BigInteger Rescale(BigInteger value, int fromScale, int toScale)
        {
            // toScale >= fromScale for every caller here (they align to the max scale).
            return toScale == fromScale ? value : value * Pow10(toScale - fromScale);
        }

        private static int Digits(BigInteger absValue)
        {
            return absValue.IsZero ? 1 : BigInteger.Abs(absValue).ToString(CultureInfo.InvariantCulture).Length;
        }

        private static void StripTrailingZeros(ref BigInteger value, ref int scale)
        {
            while (scale > 0 && !value.IsZero && value % 10 == 0)
            {
                value /= 10;
                scale--;
            }
        }

        /// <summary>Integer floor square root of a non-negative value (Newton's method).</summary>
        private static BigInteger ISqrt(BigInteger n)
        {
            if (n.Sign <= 0)
            {
                return BigInteger.Zero;
            }

            if (n < 4)
            {
                return BigInteger.One;
            }

            // Initial guess: 10^(ceil(digits/2)).
            BigInteger x = Pow10((Digits(n) + 1) / 2);
            while (true)
            {
                BigInteger y = (x + n / x) / 2;
                if (y >= x)
                {
                    break;
                }

                x = y;
            }

            // x is now >= floor(sqrt(n)); step down to the exact floor.
            while (x * x > n)
            {
                x -= 1;
            }

            return x;
        }
    }
}
