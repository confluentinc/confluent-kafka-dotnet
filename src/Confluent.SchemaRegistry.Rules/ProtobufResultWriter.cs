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
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Google.Protobuf.WellKnownTypes;
using NodaTime;
using System.Numerics;
using System.Reflection;
using SrVariant = Confluent.SchemaRegistry.Variant;

namespace Confluent.SchemaRegistry.Rules
{
    /// <summary>
    ///     Rebuilds a protobuf message from the map a message-level <c>CEL</c> transform returned.
    ///
    ///     A rule that returns a map is returning <b>the whole new message</b>: the transform has
    ///     replace semantics, not merge. Three consequences a rule author needs to know, and every
    ///     client has to match:
    ///     <list type="bullet">
    ///       <item>a field the rule does not name is <b>dropped</b>, so a rule naming only the
    ///       field it changes discards the rest;</item>
    ///       <item>a <c>null</c> in the map <b>clears</b> its field;</item>
    ///       <item>echoing a field that was absent <b>materialises</b> it, because reading it
    ///       produced a value. Preserve absence with <c>has(x) ? x : null</c>.</item>
    ///     </list>
    ///
    ///     Without this the executor handed back a plain <see cref="IDictionary" />, which the
    ///     protobuf serializer cannot write.
    ///
    ///     <para><b>Placement note.</b> This runs inside the CEL executor, as it does in every
    ///     other client. <c>Google.Protobuf</c> reaches this assembly through Cel.NET, so the only
    ///     thing out of reach is the <i>generated</i> <c>confluent.type.*</c> classes, which live
    ///     in the protobuf serde. <see cref="BuildValueType" /> therefore constructs those two
    ///     messages from their descriptor instead of naming them - the same trick the nested-message
    ///     path below already uses.</para>
    ///
    ///     <para><b>Mechanism note.</b> The JVM client rebuilds by rendering the result to JSON and
    ///     parsing it back. This builds the message directly through reflection instead, because a
    ///     JSON round trip would base64 every bytes field and format every timestamp only to parse
    ///     them straight back. The behaviours the JVM client gets free from the JSON mapping - null
    ///     clearing a field, and a key matching either the declared or the JSON name - are
    ///     reproduced explicitly below.</para>
    /// </summary>
    internal static class ProtobufResultWriter
    {
        private const string DecimalTypeName = "confluent.type.Decimal";
        private const string VariantTypeName = "confluent.type.Variant";
        private const string TimestampTypeName = "google.protobuf.Timestamp";

        /// <summary>
        ///     Rebuilds <paramref name="original" />'s type from <paramref name="result" />, or
        ///     returns <paramref name="result" /> unchanged when it is not a map.
        /// </summary>
        public static object Convert(object original, object result)
        {
            if (!(result is IDictionary values) || !(original is IMessage src))
            {
                return result;
            }

            // A fresh message of the input's own type, so the caller gets back what it passed in.
            IMessage output = (IMessage)Activator.CreateInstance(src.GetType());
            Fill(output, values);
            return output;
        }

        private static void Fill(IMessage output, IDictionary values)
        {
            MessageDescriptor desc = output.Descriptor;
            foreach (DictionaryEntry entry in values)
            {
                FieldDescriptor fd = FindField(desc, entry.Key?.ToString());
                if (fd == null)
                {
                    // A key the schema does not declare has nowhere to go. Dropping it matches
                    // the JVM client, whose JSON parse ignores unknown fields.
                    continue;
                }

                if (IsNull(entry.Value))
                {
                    // An explicit null clears the field, which is how a rule preserves an absent
                    // value across a transform that echoes it.
                    fd.Accessor.Clear(output);
                    continue;
                }

                SetField(output, fd, entry.Value);
            }
        }

        /// <summary>
        ///     Covers both shapes a CEL null takes here: a plain <c>null</c>, and the
        ///     <c>NullValue</c> the CEL runtime hands back for an explicit null literal.
        /// </summary>
        private static bool IsNull(object value)
        {
            return value == null || value is NullValue;
        }

        /// <summary>
        ///     Resolves a result key by declared name, then by JSON name: a rule may legitimately
        ///     return either, so matching only the declared name would silently skip a field like
        ///     <c>total_amount</c>.
        /// </summary>
        private static FieldDescriptor FindField(MessageDescriptor desc, string name)
        {
            if (name == null)
            {
                return null;
            }

            FieldDescriptor fd = desc.FindFieldByName(name);
            if (fd != null)
            {
                return fd;
            }

            foreach (FieldDescriptor candidate in desc.Fields.InDeclarationOrder())
            {
                if (candidate.JsonName == name)
                {
                    return candidate;
                }
            }

            return null;
        }

        private static void SetField(IMessage output, FieldDescriptor fd, object value)
        {
            if (fd.IsMap)
            {
                SetMap(output, fd, value);
                return;
            }

            if (fd.IsRepeated)
            {
                SetRepeated(output, fd, value);
                return;
            }

            if (fd.FieldType == FieldType.Message)
            {
                IMessage nested = BuildMessage(fd.MessageType, value);
                if (nested != null)
                {
                    fd.Accessor.SetValue(output, nested);
                }

                return;
            }

            fd.Accessor.SetValue(output, Scalar(fd, value));
        }

        private static void SetMap(IMessage output, FieldDescriptor fd, object value)
        {
            // A shape mismatch is an error, not a no-op. The message is rebuilt field by field,
            // so returning here left the map *empty* - a rule that answered with a scalar or a
            // list silently discarded the data instead of reporting anything, which is the one
            // outcome the rest of this writer is careful to avoid (every Scalar arm throws).
            // Measured against protobuf-java 4.34.0's JsonFormat, which is the reference for
            // this path - the JVM renders the CEL result map to protobuf JSON and parses it:
            //   {"m": "notamap"} -> Expect a map object but found: "notamap"
            //   {"m": [1,2]}     -> Expect a map object but found: [1,2]
            if (!(value is IDictionary entries))
            {
                throw Mismatch(fd, value, "map");
            }

            var target = (IDictionary)fd.Accessor.GetValue(output);
            FieldDescriptor keyFd = fd.MessageType.FindFieldByName("key");
            FieldDescriptor valueFd = fd.MessageType.FindFieldByName("value");
            foreach (DictionaryEntry entry in entries)
            {
                if (IsNull(entry.Value))
                {
                    continue;
                }

                // The key needs narrowing just as the value does: CEL carries an integer key as
                // a long, and MapField<int, ...> rejects a boxed long.
                target[Scalar(keyFd, entry.Key)] = valueFd.FieldType == FieldType.Message
                    ? (object)BuildMessage(valueFd.MessageType, entry.Value)
                    : Scalar(valueFd, entry.Value);
            }
        }

        private static void SetRepeated(IMessage output, FieldDescriptor fd, object value)
        {
            // Same as SetMap: silently leaving the list empty is the one outcome to avoid. The
            // `value is string` test comes first because a string *is* IEnumerable in .NET, so
            // it would otherwise be spread into one element per character. JsonFormat refuses
            // both shapes, measured:
            //   {"r": "notalist"} -> Expected an array for r but found "notalist"
            //   {"r": {"a":1}}    -> Expected an array for r but found {"a":1}
            if (value is string || !(value is IEnumerable items))
            {
                throw Mismatch(fd, value, "list");
            }

            var target = (IList)fd.Accessor.GetValue(output);
            target.Clear();
            foreach (object item in items)
            {
                if (IsNull(item))
                {
                    continue;
                }

                target.Add(fd.FieldType == FieldType.Message
                    ? (object)BuildMessage(fd.MessageType, item)
                    : Scalar(fd, item));
            }
        }

        /// <summary>
        ///     Builds one message value, inverting how the CEL binding read it. The three value
        ///     types do not arrive as maps of their own fields: this client binds a decimal as
        ///     <see cref="BigDecimal" /> and a timestamp as a NodaTime <c>ZonedDateTime</c>, so
        ///     that is what comes back whether the rule computed a new value or echoed the field.
        ///     A variant arrives as the proto message when echoed and as
        ///     <see cref="SrVariant" /> when computed.
        /// </summary>
        private static IMessage BuildMessage(MessageDescriptor desc, object value)
        {
            // A message echoed straight through.
            if (value is IMessage message)
            {
                return message;
            }

            string fullName = desc.FullName;

            if (value is BigDecimal bigDecimal)
            {
                Require(fullName == DecimalTypeName, "a decimal", fullName);
                return BuildDecimal(desc, bigDecimal);
            }

            if (value is decimal dec)
            {
                Require(fullName == DecimalTypeName, "a decimal", fullName);
                return BuildDecimal(desc, BigDecimal.FromDecimal(dec));
            }

            // The CEL runtime hands timestamps back as NodaTime values.
            if (value is ZonedDateTime zoned)
            {
                Require(fullName == TimestampTypeName, "a timestamp", fullName);
                return FromInstant(zoned.ToInstant());
            }

            if (value is Instant instant)
            {
                Require(fullName == TimestampTypeName, "a timestamp", fullName);
                return FromInstant(instant);
            }

            if (value is DateTimeOffset offset)
            {
                Require(fullName == TimestampTypeName, "a timestamp", fullName);
                return Timestamp.FromDateTimeOffset(offset);
            }

            if (value is SrVariant variant)
            {
                Require(fullName == VariantTypeName, "a variant", fullName);
                IMessage output = BuildValueType(desc);
                SetByName(output, "metadata", ByteString.CopyFrom(variant.MetadataBytes));
                SetByName(output, "value", ByteString.CopyFrom(variant.ValueBytes));
                return output;
            }

            // A nested message the rule rebuilt field by field.
            if (value is IDictionary nested)
            {
                var built = (IMessage)Activator.CreateInstance(
                    desc.ClrType ?? throw new RuleException(
                        "cannot rebuild " + fullName + ": no CLR type"));
                Fill(built, nested);
                return built;
            }

            throw new RuleException(
                "cannot write " + value.GetType().Name + " to " + fullName);
        }

        /// <summary>
        ///     Builds <c>confluent.type.Decimal</c> the way
        ///     <c>DecimalExtensions.ToProtobufDecimal</c> does, but through the descriptor: the
        ///     unscaled magnitude as big-endian two's complement, plus the scale and the
        ///     precision. Precision is the unscaled value's digit count, which is what
        ///     <c>BigDecimal.precision()</c> reports and what the JVM's ProtobufResultWriter
        ///     writes; leaving it unset made the same computed decimal serialize differently
        ///     here than on the JVM.
        /// </summary>
        private static IMessage BuildDecimal(MessageDescriptor desc, BigDecimal value)
        {
            byte[] buffer = value.Unscaled.ToByteArray();  // little-endian two's complement, minimal
            Array.Reverse(buffer);                          // big-endian wire form
            IMessage output = BuildValueType(desc);
            SetByName(output, "value", ByteString.CopyFrom(buffer));
            SetByName(output, "precision", UnscaledPrecision(value.Unscaled));
            SetByName(output, "scale", value.Scale);
            return output;
        }

        /// <summary>
        ///     The digit count of an unscaled value, which is what <c>BigDecimal.precision()</c>
        ///     reports. Zero has precision 1 there.
        /// </summary>
        /// <remarks>
        ///     Delegates to <see cref="BigDecimal.UnscaledPrecision" />, which carries the
        ///     coefficient-width guard. This path had its own unguarded copy, so a decimal a
        ///     rule *computed* could drive an unbounded quadratic <c>BigInteger.ToString()</c>
        ///     here while the identical value was refused by the serde's write-back - the same
        ///     client accepting and rejecting one decimal depending on which path produced it.
        /// </remarks>
        internal static uint UnscaledPrecision(BigInteger unscaled)
            => BigDecimal.UnscaledPrecision(unscaled);

        /// <summary>
        ///     A fresh instance of a generated message, obtained from its descriptor. The generated
        ///     <c>confluent.type.Decimal</c> and <c>confluent.type.Variant</c> classes live in the
        ///     protobuf serde assembly, which this one does not reference; the descriptor reached
        ///     here from the field being written, and it carries the CLR type.
        /// </summary>
        private static IMessage BuildValueType(MessageDescriptor desc)
        {
            return (IMessage)Activator.CreateInstance(
                desc.ClrType ?? throw new RuleException(
                    "cannot rebuild " + desc.FullName + ": no CLR type"));
        }

        /// <summary>
        ///     Sets one field by its declared name. Named rather than numbered so a mismatch fails
        ///     loudly here instead of writing the wrong field.
        /// </summary>
        private static void SetByName(IMessage output, string name, object value)
        {
            FieldDescriptor fd = output.Descriptor.FindFieldByName(name);
            if (fd == null)
            {
                throw new RuleException(
                    "cannot rebuild " + output.Descriptor.FullName + ": no field '" + name + "'");
            }

            fd.Accessor.SetValue(output, value);
        }

        private static void Require(bool ok, string what, string fullName)
        {
            if (!ok)
            {
                throw new RuleException("cannot write " + what + " to " + fullName);
            }
        }

        /// <summary>
        ///     A protobuf timestamp from a NodaTime instant, keeping every digit.
        ///     <c>ToUnixTimeSecondsAndNanoseconds</c> truncates the seconds towards the start of
        ///     time so the nanoseconds are non-negative, which is exactly protobuf's contract.
        ///     Converting through <see cref="DateTimeOffset" /> instead rounded to its
        ///     100-nanosecond tick, turning a <c>Nanos</c> of 123456789 into 123456700.
        /// </summary>
        private static Timestamp FromInstant(Instant instant)
        {
            var (seconds, nanoseconds) = instant.ToUnixTimeSecondsAndNanoseconds();
            return new Timestamp { Seconds = seconds, Nanos = (int)nanoseconds };
        }

        /// <summary>
        ///     Narrows a CEL value to what the field's type accepts. The CEL runtime widens every
        ///     integer to <c>long</c> and every float to <c>double</c>, so a narrower field needs
        ///     converting back - but only where the conversion is exact, and only from a value of
        ///     the field's own kind.
        ///
        ///     <para>Every arm was a bare <c>System.Convert.To*</c>, which coerces rather than
        ///     checks, so a wrong-typed or inexact result was accepted and silently changed:
        ///     <c>ToInt32(1.9)</c> gave <b>2</b> (and half-to-even, so 2.5 also gave 2),
        ///     <c>ToBoolean(0)</c> gave false, <c>ToBoolean("TRUE")</c> gave true,
        ///     <c>ToSingle(true)</c> gave 1, <c>ToSingle(1e40)</c> gave +Infinity, and
        ///     <c>ToString()</c> wrote any value at all into a string field as its .NET text.</para>
        ///
        ///     <para>The contract is protobuf's own JSON parser, which is what the JVM's
        ///     write-back parses the result map with. Measured against protobuf-java 4.35.1:
        ///     <c>int32 &lt;- 1.9</c> and <c>&lt;- true</c> are refused ("Not an int32 value"),
        ///     <c>int32 &lt;- 2.0</c> gives 2, <c>bool &lt;- 0</c> and <c>&lt;- "TRUE"</c> are
        ///     refused ("Invalid bool value"), <c>float &lt;- 1.0e40</c> is refused ("Out of range
        ///     float value"), and <c>double &lt;- 3</c> gives 3.0.</para>
        ///
        ///     <para>That parser is also lenient the other way - it stringifies a number into a
        ///     string field, reads "true"/"false" as a bool and a numeric string as a number - and
        ///     none of that is followed here. Those coercions exist only because its input crossed
        ///     a JSON transport, which this writer does not cross, and each one turns a
        ///     rule-authoring mistake into silently wrong data.</para>
        /// </summary>
        private static object Scalar(FieldDescriptor fd, object value)
        {
            switch (fd.FieldType)
            {
                case FieldType.Bool:
                    if (!(value is bool flag))
                    {
                        throw Mismatch(fd, value, "bool");
                    }

                    return flag;
                case FieldType.String:
                    if (!(value is string text))
                    {
                        throw Mismatch(fd, value, "string");
                    }

                    return text;
                case FieldType.Bytes:
                    if (value is ByteString bs)
                    {
                        return bs;
                    }

                    if (value is byte[] raw)
                    {
                        return ByteString.CopyFrom(raw);
                    }

                    throw Mismatch(fd, value, "bytes");
                case FieldType.Float:
                    return NarrowToFloat(fd, Floating(fd, value));
                case FieldType.Double:
                    return Floating(fd, value);
                case FieldType.Enum:
                    // CEL carries an enum as its number, but the reflection accessor assigns to
                    // the generated enum-typed property, so a boxed int is an invalid cast.
                    return System.Enum.ToObject(fd.EnumType.ClrType,
                        (int)Bounded(fd, Integral(fd, value), int.MinValue, int.MaxValue));
                case FieldType.Int32:
                case FieldType.SInt32:
                case FieldType.SFixed32:
                    return (int)Bounded(fd, Integral(fd, value), int.MinValue, int.MaxValue);
                case FieldType.UInt32:
                case FieldType.Fixed32:
                    return (uint)Bounded(fd, Integral(fd, value), uint.MinValue, uint.MaxValue);
                case FieldType.UInt64:
                case FieldType.Fixed64:
                    return (ulong)Bounded(fd, Integral(fd, value), ulong.MinValue, ulong.MaxValue);
                default:
                    return (long)Bounded(fd, Integral(fd, value), long.MinValue, long.MaxValue);
            }
        }

        /// <summary>
        ///     The CEL value as an exact integer. Carried as a <see cref="decimal" /> because it
        ///     has to hold the whole signed <em>and</em> unsigned 64-bit domain, which neither
        ///     <c>long</c> nor <c>ulong</c> does; the caller's <c>Convert.To*</c> then range-checks
        ///     it for the field's own width and throws <see cref="OverflowException" />.
        /// </summary>
        private static decimal Integral(FieldDescriptor fd, object value)
        {
            switch (value)
            {
                case bool _:
                    // protobuf JSON refuses true for an integer field, and .NET would write 1.
                    throw new RuleException("cannot write bool to integer field " + fd.FullName);
                case sbyte _:
                case byte _:
                case short _:
                case ushort _:
                case int _:
                case uint _:
                case long _:
                case ulong _:
                    return System.Convert.ToDecimal(value, CultureInfo.InvariantCulture);
                case float f:
                    return ExactlyIntegral(fd, f);
                case double d:
                    return ExactlyIntegral(fd, d);
                default:
                    throw Mismatch(fd, value, "integer");
            }
        }

        /// <summary>
        ///     A floating value as an integer, only when it is exactly integral. A fractional
        ///     value is a rule-authoring mistake rather than something to round: rounding writes a
        ///     different number than the rule computed. An integral one is accepted, as protobuf
        ///     JSON accepts 2.0 for an int32.
        /// </summary>
        private static decimal ExactlyIntegral(FieldDescriptor fd, double d)
        {
            if (double.IsNaN(d) || double.IsInfinity(d) || Math.Truncate(d) != d)
            {
                throw new RuleException("cannot write non-integral "
                    + d.ToString(CultureInfo.InvariantCulture)
                    + " to integer field " + fd.FullName);
            }

            // Outside the decimal range is outside every protobuf integer range too, and the
            // cast reports it as an OverflowException the same way the narrowing below does.
            return (decimal)d;
        }

        /// <summary>
        ///     The CEL value as a double. A bool is refused rather than written as 1, matching
        ///     protobuf JSON ("Not a double value: true") and the integer arm above.
        /// </summary>
        private static double Floating(FieldDescriptor fd, object value)
        {
            switch (value)
            {
                case bool _:
                    throw new RuleException("cannot write bool to float field " + fd.FullName);
                case double d:
                    return d;
                case float f:
                    return f;
                case sbyte _:
                case byte _:
                case short _:
                case ushort _:
                case int _:
                case uint _:
                case long _:
                case ulong _:
                    return System.Convert.ToDouble(value, CultureInfo.InvariantCulture);
                default:
                    throw Mismatch(fd, value, "float");
            }
        }

        /// <summary>
        ///     Narrows a double the way <c>JsonFormat.parseFloat</c> does: a finite value outside
        ///     the float range is an error rather than an infinity, with the same 1e-6 slack that
        ///     method allows. NaN and the infinities pass through - it accepts those explicitly.
        /// </summary>
        private static float NarrowToFloat(FieldDescriptor fd, double d)
        {
            const double epsilon = 1e-6;
            double limit = float.MaxValue * (1 + epsilon);
            if (!double.IsNaN(d) && !double.IsInfinity(d) && (d > limit || d < -limit))
            {
                throw new RuleException("out of range float value for " + fd.FullName + ": "
                    + d.ToString(CultureInfo.InvariantCulture));
            }

            return (float)d;
        }

        /// <summary>
        ///     Range-checks an integer for the field's own width. Reported as a
        ///     <see cref="RuleException" /> naming the field and the value, rather than as the
        ///     bare <see cref="OverflowException" /> the narrowing conversion would raise, which
        ///     names neither. The decimal carrier is exact over both 64-bit domains, so the cast
        ///     that follows a passing check cannot itself overflow.
        /// </summary>
        private static decimal Bounded(FieldDescriptor fd, decimal i, decimal min, decimal max)
        {
            if (i < min || i > max)
            {
                throw new RuleException("value " + i.ToString(CultureInfo.InvariantCulture)
                    + " is out of range for field " + fd.FullName);
            }

            return i;
        }

        private static RuleException Mismatch(FieldDescriptor fd, object value, string kind)
        {
            return new RuleException("cannot write "
                + (value == null ? "null" : value.GetType().Name)
                + " to " + kind + " field " + fd.FullName);
        }
    }
}
