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
            if (!(value is IDictionary entries))
            {
                return;
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
            if (value is string || !(value is IEnumerable items))
            {
                return;
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
        internal static uint UnscaledPrecision(BigInteger unscaled)
        {
            if (unscaled.IsZero)
            {
                return 1;
            }

            return (uint)BigInteger.Abs(unscaled).ToString(CultureInfo.InvariantCulture).Length;
        }

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
        ///     Narrows a CEL value to what the field's type accepts. The CEL runtime widens every
        ///     integer to <c>long</c> and every float to <c>double</c>, so a narrower field needs
        ///     converting back rather than rejecting.
        /// </summary>
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

        private static object Scalar(FieldDescriptor fd, object value)
        {
            switch (fd.FieldType)
            {
                case FieldType.Bool:
                    return System.Convert.ToBoolean(value);
                case FieldType.String:
                    return value.ToString();
                case FieldType.Bytes:
                    return value is ByteString bs ? bs : ByteString.CopyFrom((byte[])value);
                case FieldType.Float:
                    return System.Convert.ToSingle(value);
                case FieldType.Double:
                    return System.Convert.ToDouble(value);
                case FieldType.Enum:
                    // CEL carries an enum as its number, but the reflection accessor assigns to
                    // the generated enum-typed property, so a boxed int is an invalid cast.
                    return System.Enum.ToObject(
                        fd.EnumType.ClrType, System.Convert.ToInt32(value));
                case FieldType.Int32:
                case FieldType.SInt32:
                case FieldType.SFixed32:
                    return System.Convert.ToInt32(value);
                case FieldType.UInt32:
                case FieldType.Fixed32:
                    return System.Convert.ToUInt32(value);
                case FieldType.UInt64:
                case FieldType.Fixed64:
                    return System.Convert.ToUInt64(value);
                default:
                    return System.Convert.ToInt64(value);
            }
        }
    }
}
