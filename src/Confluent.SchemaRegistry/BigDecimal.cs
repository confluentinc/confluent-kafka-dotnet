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

        /// <summary>
        ///     The width ceiling for a computation, in decimal digits.
        /// </summary>
        /// <remarks>
        ///     Deliberately <b>not</b> Java's - <c>BigInteger</c> tops out at
        ///     <c>Integer.MAX_VALUE</c> bits, which is 646456993 digits, and reproducing that
        ///     bound across six decimal libraries is neither achievable nor the point. This is a
        ///     round number chosen so no single rule evaluation can exhaust memory. Width
        ///     failure is the one thing that cannot be turned into a rule error after the fact:
        ///     <c>BigInteger</c> here is unbounded until the process dies, where Java raises an
        ///     <c>ArithmeticException</c>. Measured on the shared libmpdec in the Python client,
        ///     peak RSS on operands 1e2147483647 and 3: <c>mul</c>, <c>div</c>, comparison,
        ///     negation and <c>abs</c> all 13 MB; <c>add</c> 1738 MB, <c>sub</c> 1738 MB,
        ///     <c>remainder</c> 1733 MB. So the guards follow <i>exponent alignment</i>, not
        ///     arithmetic - the operations that have to build a positional form.
        /// </remarks>
        public const int SaneWidth = 10_000_000;

        /// <summary>
        ///     A far tighter ceiling on what can be <i>encoded</i>, which bounds a different
        ///     resource.
        /// </summary>
        /// <remarks>
        ///     <c>confluent.type.Decimal.value</c> is the unscaled integer in base 256, and
        ///     decimal to binary radix conversion is quadratic in every client - here it is
        ///     <c>BigInteger.ToString()</c>, which <see cref="Digits" /> and the wire encoder
        ///     both call. 4300 is CPython's own <c>int_max_str_digits</c>, the cap it puts on
        ///     string/integer conversion for exactly this reason; the Python, C++ and JS clients
        ///     all adopt it, so every client agrees on which decimals can be written. CEL's
        ///     documented decimal precision is 38 digits, so this leaves two orders of headroom
        ///     over anything a rule is meant to produce.
        /// </remarks>
        public const int SaneCoefficient = 4300;

        private static readonly BigInteger MaxDecimalValue = new BigInteger(decimal.MaxValue);
        private static readonly BigInteger MinDecimalValue = new BigInteger(decimal.MinValue);

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
        ///     Lossless conversion from a <see cref="decimal" />, built directly from its bits so
        ///     that scale and trailing zeros are preserved (<c>1.50m</c> becomes scale 2).
        /// </summary>
        public static BigDecimal FromDecimal(decimal value)
        {
            int[] bits = decimal.GetBits(value);           // [lo, mid, hi, flags]
            BigInteger unscaled = (new BigInteger((uint)bits[2]) << 64)
                                | (new BigInteger((uint)bits[1]) << 32)
                                | new BigInteger((uint)bits[0]);
            int scale = (bits[3] >> 16) & 0xFF;            // scale is bits 16-23
            if ((bits[3] & unchecked((int)0x80000000)) != 0) unscaled = -unscaled; // sign bit 31
            return new BigDecimal(unscaled, scale);
        }

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

            return Parse(EnsureFractional(value.ToString("R", CultureInfo.InvariantCulture)));
        }

        /// <summary>
        ///     A <see cref="BigDecimal" /> from a <see cref="float" />, via the shortest decimal
        ///     string that round-trips to the same float — Java <c>Float.toString</c>. A whole-number
        ///     value keeps a trailing <c>.0</c> (scale 1), matching Java/Python and the other clients.
        /// </summary>
        public static BigDecimal FromFloat(float value)
        {
            if (float.IsNaN(value) || float.IsInfinity(value))
            {
                throw new ArgumentException($"Cannot convert {value} to Decimal");
            }

            return Parse(EnsureFractional(value.ToString("R", CultureInfo.InvariantCulture)));
        }

        // Java Double/Float.toString always emit a fractional digit for a whole number ("2.0",
        // scale 1); .NET's "R" format omits it ("2", scale 0), which would diverge from the other
        // clients and propagate through multiply. Restore the ".0" unless the value is in
        // scientific notation.
        private static string EnsureFractional(string s)
        {
            if (s.IndexOf('.') < 0 && s.IndexOf('E') < 0 && s.IndexOf('e') < 0)
            {
                return s + ".0";
            }
            return s;
        }

        // ---- Arithmetic --------------------------------------------------------------

        public BigDecimal Add(BigDecimal other)
        {
            RequireAlignable(this, other, "decimals.add");
            int s = Math.Max(scale, other.scale);
            return new BigDecimal(Rescale(unscaled, scale, s) + Rescale(other.unscaled, other.scale, s), s);
        }

        public BigDecimal Subtract(BigDecimal other)
        {
            RequireAlignable(this, other, "decimals.sub");
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

            BigInteger resultUnscaled = sign < 0 ? -q : q;
            int resultScale = s - baseShift;
            if (exact)
            {
                // Java targets the preferred scale (dividend.scale - divisor.scale) for an
                // exact result: strip trailing zeros only down to it, and pad back up to it
                // when the natural scale is smaller. Never strip below the preferred scale
                // (6.0/3 -> "2.0", not "2"; 10.00/2 -> "5.00").
                ApplyPreferredScale(ref resultUnscaled, ref resultScale, scale - divisor.scale);
            }

            return new BigDecimal(resultUnscaled, resultScale);
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

            RequireAlignable(this, divisor, "decimals.mod");
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
                // Java targets the preferred scale (radicand.scale / 2) for an exact result:
                // strip trailing zeros only down to it, padding back up when the natural scale
                // is smaller (sqrt(4.00) -> "2.0"; sqrt(100.0000) -> "10.00").
                ApplyPreferredScale(ref q, ref s, scale / 2);
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
            // Pow10 is built whichever way the scale moves - it is the multiplier when
            // expanding and the divisor when coarsening - so unlike libmpdec's rescale, both
            // directions cost `|newScale - scale|` digits here, and an expanding one also costs
            // the resulting coefficient. Zero is not exempt for that reason: the power of ten
            // is materialised even when the value it scales is zero.
            RequireSaneWidth(
                Math.Max((long)Math.Abs((long)newScale - scale),
                         (long)Digits(unscaled) + ((long)newScale - scale)),
                "decimal", $"a scale of {newScale}");
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
            // Sign first, then magnitude, and only then the aligned comparison. Going straight
            // to Rescale made comparison as expensive as addition - `1e-2000000000 < 1` had to
            // build a two-billion-digit power of ten - where libmpdec short-circuits on the
            // adjusted exponent and Java's compareTo does the same. Comparison is the one
            // operation that must not fail on width: it is how a rule inspects a field, and a
            // guard here would break the ordinary case of comparing values of different scales.
            int signA = unscaled.Sign;
            int signB = other.unscaled.Sign;
            if (signA != signB)
            {
                return signA < signB ? -1 : 1;
            }

            if (signA == 0)
            {
                return 0;
            }

            // The adjusted exponent - the power of ten of the leading digit. Where they differ
            // the larger magnitude wins outright, with the sign deciding the direction.
            long adjustedA = (long)Digits(unscaled) - 1 - scale;
            long adjustedB = (long)Digits(other.unscaled) - 1 - other.scale;
            if (adjustedA != adjustedB)
            {
                bool aBigger = adjustedA > adjustedB;
                return (aBigger == (signA > 0)) ? 1 : -1;
            }

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
            if (q.IsZero)
            {
                sc = 0; // every zero is value-equal regardless of scale
            }
            else
            {
                StripTrailingZeros(ref q, ref sc);
            }
            return q.GetHashCode() * 397 ^ sc;
        }

        // ---- Conversion --------------------------------------------------------------

        /// <summary>
        ///     Plain decimal string with no scientific notation — Java <c>toPlainString</c>.
        /// </summary>
        public string ToPlainString()
        {
            // The plain form pays for the scale in *both* directions: a negative scale appends
            // that many trailing zeros and a scale wider than the coefficient prepends that
            // many leading ones. So a one-digit coefficient at an extreme scale still renders
            // enormous, which is reachable without any rescale at all - Divide holds its
            // coefficient to 38 digits while the scale runs free.
            RequireSaneWidth((long)Digits(unscaled) + Math.Abs((long)scale), "string",
                "the plain form");
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

        /// <summary>
        ///     Nearest <see cref="decimal" /> (may lose precision). Throws
        ///     <see cref="OverflowException" /> when the integer part does not fit in a
        ///     <see cref="decimal" /> — Java <c>toBigDecimal</c> narrowed to System.Decimal.
        /// </summary>
        public decimal ToDecimal()
        {
            BigInteger uns = unscaled;
            int sc = scale;
            if (sc < 0)
            {
                uns *= BigInteger.Pow(10, -sc);
                sc = 0;
            }
            else if (sc > 28)
            {
                // System.Decimal holds at most 28 fractional digits; round the excess away with
                // HALF_UP so an exactly-representable high-scale value (e.g. 1.5 stored at scale 30)
                // converts instead of overflowing the 10^sc divisor, and a sub-1e-28 value rounds to 0.
                BigInteger dropDivisor = BigInteger.Pow(10, sc - 28);
                BigInteger q = BigInteger.DivRem(uns, dropDivisor, out BigInteger dropRem);
                if (BigInteger.Abs(dropRem) * 2 >= dropDivisor)
                {
                    q += uns.Sign; // round half away from zero
                }
                uns = q;
                sc = 28;
            }

            BigInteger scaleDivisor = BigInteger.Pow(10, sc);
            BigInteger quotient = BigInteger.DivRem(uns, scaleDivisor, out BigInteger remainder);
            if (quotient > MaxDecimalValue || quotient < MinDecimalValue)
            {
                throw new OverflowException("The value cannot fit into System.Decimal.");
            }
            return (decimal)quotient + (decimal)remainder / (decimal)scaleDivisor;
        }

        public override string ToString() => ToPlainString();

        // ---- Helpers -----------------------------------------------------------------

        private static BigInteger Pow10(int n) => BigInteger.Pow(10, n);

        /// <summary>Refuses a positional form too wide to build.</summary>
        public static void RequireSaneWidth(long needed, string fn, string what, int limit = SaneWidth)
        {
            if (needed > limit)
            {
                throw new ArithmeticException(
                    $"{fn}: {what} needs {needed} digits, past this client's {limit}-digit limit");
            }
        }

        /// <summary>
        ///     The exponent gap two operands are aligned across, which is what
        ///     <see cref="Rescale" /> has to build a power of ten for.
        /// </summary>
        private static void RequireAlignable(BigDecimal a, BigDecimal b, string fn)
        {
            long target = Math.Max(a.scale, b.scale);
            long widest = Math.Max(
                (long)Digits(a.unscaled) + (target - a.scale),
                (long)Digits(b.unscaled) + (target - b.scale));
            RequireSaneWidth(widest + 1, fn, "aligning the operands");
        }

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

        /// <summary>
        ///     Rewrite an exact div/sqrt result to Java <c>BigDecimal</c>'s preferred scale:
        ///     strip trailing zeros down to (but never below) <paramref name="preferredScale" />,
        ///     then pad with trailing zeros back up to it when the natural scale is smaller.
        ///     Mirrors Go's <c>applyPreferredScale</c>. The preferred scale is
        ///     <c>dividend.scale - divisor.scale</c> for division and <c>radicand.scale / 2</c>
        ///     for square root.
        /// </summary>
        private static void ApplyPreferredScale(ref BigInteger value, ref int scale, int preferredScale)
        {
            while (scale > preferredScale && !value.IsZero && value % 10 == 0)
            {
                value /= 10;
                scale--;
            }

            if (scale < preferredScale)
            {
                value *= Pow10(preferredScale - scale);
                scale = preferredScale;
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
