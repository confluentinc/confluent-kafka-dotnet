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
using System.Collections.Generic;
using System.Globalization;
using System.Numerics;
using System.Text;
using Newtonsoft.Json.Linq;

namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     A flat, streaming writer that builds a <see cref="Variant" /> programmatically. A single
    ///     builder maintains an internal nesting stack (the arrow-dotnet flat streaming-writer shape):
    ///     each <c>Append*</c> targets the current slot - the root, the next array element, or the
    ///     current object field value (after <see cref="AppendKey" />). Object fields are sorted by key
    ///     on <see cref="EndObject" /> (canonical order) and their keys accumulate into the metadata
    ///     dictionary. <see cref="Build" /> finalizes and returns a <see cref="Variant" />.
    ///
    ///     <para>Output is byte-identical to <see cref="Variant.ParseJson" /> for the equivalent
    ///     document: <see cref="Variant.ParseJson" /> is itself implemented on top of this builder.
    ///     Integer widths are the caller's choice here (<see cref="AppendByte" /> ..
    ///     <see cref="AppendLong" />), whereas the JSON path selects the smallest width that fits; pick
    ///     the matching method to reproduce a parsed document exactly.</para>
    ///
    ///     <para>Misuse (an unbalanced <c>Start*</c>/<c>End*</c>, <see cref="AppendKey" /> outside an
    ///     object, a value append without a preceding <see cref="AppendKey" /> inside an object, or
    ///     <see cref="Build" /> with an open container) raises <see cref="VariantException" />.</para>
    /// </summary>
    public sealed class VariantBuilder
    {
        private readonly List<byte> value = new List<byte>();
        private readonly Dictionary<string, int> dictionary = new Dictionary<string, int>();
        private readonly List<byte[]> dictionaryKeys = new List<byte[]>();
        private readonly List<Frame> stack = new List<Frame>();
        private bool rootWritten;

        // --- scalar appends (target the current slot) ---

        /// <summary>Append a null value into the current slot.</summary>
        public void AppendNull()
        {
            BeforeValue();
            value.Add(PrimitiveHeader(Variant.TNull));
        }

        /// <summary>Append a boolean value.</summary>
        public void AppendBoolean(bool b)
        {
            BeforeValue();
            value.Add(PrimitiveHeader(b ? Variant.TTrue : Variant.TFalse));
        }

        /// <summary>Append a signed 8-bit integer (INT8).</summary>
        public void AppendByte(sbyte v)
        {
            BeforeValue();
            value.Add(PrimitiveHeader(Variant.TInt1));
            AppendLongLE(value, v, 1);
        }

        /// <summary>Append a signed 16-bit integer (INT16).</summary>
        public void AppendShort(short v)
        {
            BeforeValue();
            value.Add(PrimitiveHeader(Variant.TInt2));
            AppendLongLE(value, v, 2);
        }

        /// <summary>Append a signed 32-bit integer (INT32).</summary>
        public void AppendInt(int v)
        {
            BeforeValue();
            value.Add(PrimitiveHeader(Variant.TInt4));
            AppendLongLE(value, v, 4);
        }

        /// <summary>Append a signed 64-bit integer (INT64).</summary>
        public void AppendLong(long v)
        {
            BeforeValue();
            value.Add(PrimitiveHeader(Variant.TInt8));
            AppendLongLE(value, v, 8);
        }

        /// <summary>Append a 32-bit float (FLOAT).</summary>
        public void AppendFloat(float f)
        {
            BeforeValue();
            value.Add(PrimitiveHeader(Variant.TFloat));
            byte[] b = BitConverter.GetBytes(f);
            if (!BitConverter.IsLittleEndian) Array.Reverse(b);
            value.AddRange(b);
        }

        /// <summary>Append a 64-bit double (DOUBLE).</summary>
        public void AppendDouble(double d)
        {
            BeforeValue();
            WriteDouble(d);
        }

        /// <summary>
        ///     Append a decimal from its unscaled value (two's-complement big-endian bytes) and scale.
        ///     The narrowest of DECIMAL4/8/16 that holds the precision is chosen, matching the JSON
        ///     path.
        /// </summary>
        public void AppendDecimal(byte[] unscaledBigEndian, int scale)
        {
            if (unscaledBigEndian == null) throw new ArgumentNullException(nameof(unscaledBigEndian));
            BeforeValue();
            WriteDecimal(BigIntegerFromBigEndian(unscaledBigEndian), scale);
        }

        /// <summary>Append a decimal from a <see cref="BigInteger" /> unscaled value and scale.</summary>
        public void AppendDecimal(BigInteger unscaled, int scale)
        {
            BeforeValue();
            WriteDecimal(unscaled, scale);
        }

        /// <summary>Append a decimal from a <see cref="BigDecimal" />.</summary>
        public void AppendDecimal(BigDecimal d)
        {
            BeforeValue();
            WriteDecimal(d.Unscaled, d.Scale);
        }

        /// <summary>Append a string (auto short-string when &lt;= 63 UTF-8 bytes, else long-string).</summary>
        public void AppendString(string s)
        {
            if (s == null) throw new ArgumentNullException(nameof(s));
            BeforeValue();
            WriteString(s);
        }

        /// <summary>Append an opaque binary blob.</summary>
        public void AppendBinary(byte[] bytes)
        {
            if (bytes == null) throw new ArgumentNullException(nameof(bytes));
            BeforeValue();
            value.Add(PrimitiveHeader(Variant.TBinary));
            AppendUintLE(value, bytes.Length, Variant.U32Size);
            value.AddRange(bytes);
        }

        /// <summary>Append a UUID from its 16 canonical big-endian bytes.</summary>
        public void AppendUuid(byte[] uuid16)
        {
            if (uuid16 == null) throw new ArgumentNullException(nameof(uuid16));
            if (uuid16.Length != 16) throw new VariantException("uuid must be 16 bytes");
            BeforeValue();
            value.Add(PrimitiveHeader(Variant.TUuid));
            value.AddRange(uuid16);
        }

        /// <summary>Append a DATE (days since the Unix epoch).</summary>
        public void AppendDate(int daysSinceEpoch)
        {
            BeforeValue();
            value.Add(PrimitiveHeader(Variant.TDate));
            AppendLongLE(value, daysSinceEpoch, 4);
        }

        /// <summary>Append a TIME_NTZ (microseconds since midnight).</summary>
        public void AppendTime(long microsSinceMidnight)
        {
            BeforeValue();
            value.Add(PrimitiveHeader(Variant.TTime));
            AppendLongLE(value, microsSinceMidnight, 8);
        }

        /// <summary>Append a TIMESTAMP with time zone (microseconds since the Unix epoch).</summary>
        public void AppendTimestampTz(long micros)
        {
            BeforeValue();
            value.Add(PrimitiveHeader(Variant.TTimestamp));
            AppendLongLE(value, micros, 8);
        }

        /// <summary>Append a TIMESTAMP without time zone (microseconds since the Unix epoch).</summary>
        public void AppendTimestampNtz(long micros)
        {
            BeforeValue();
            value.Add(PrimitiveHeader(Variant.TTimestampNtz));
            AppendLongLE(value, micros, 8);
        }

        /// <summary>Append a nanosecond TIMESTAMP with time zone (nanoseconds since the Unix epoch).</summary>
        public void AppendTimestampNanosTz(long nanos)
        {
            BeforeValue();
            value.Add(PrimitiveHeader(Variant.TTimestampNanos));
            AppendLongLE(value, nanos, 8);
        }

        /// <summary>Append a nanosecond TIMESTAMP without time zone (nanoseconds since the Unix epoch).</summary>
        public void AppendTimestampNanosNtz(long nanos)
        {
            BeforeValue();
            value.Add(PrimitiveHeader(Variant.TTimestampNanosNtz));
            AppendLongLE(value, nanos, 8);
        }

        // --- containers ---

        /// <summary>Begin an object; subsequent field values are preceded by <see cref="AppendKey" />.</summary>
        public void StartObject()
        {
            BeforeValue();
            stack.Add(Frame.NewObject(value.Count));
        }

        /// <summary>Set the key for the next appended value within the current object.</summary>
        public void AppendKey(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            Frame f = Top();
            if (f == null || !f.IsObject)
            {
                throw new VariantException("VariantBuilder: AppendKey called outside an object");
            }
            if (f.HasPendingKey)
            {
                throw new VariantException("VariantBuilder: AppendKey called twice without a value");
            }
            f.PendingId = AddKey(key);
            f.PendingKey = key;
            f.HasPendingKey = true;
        }

        /// <summary>End the current object (its fields are sorted by key).</summary>
        public void EndObject()
        {
            Frame f = Top();
            if (f == null || !f.IsObject)
            {
                throw new VariantException("VariantBuilder: EndObject with no matching StartObject");
            }
            if (f.HasPendingKey)
            {
                throw new VariantException("VariantBuilder: EndObject with a dangling AppendKey");
            }
            stack.RemoveAt(stack.Count - 1);
            FinishWritingObject(f.Start, f.Fields);
        }

        /// <summary>Begin an array; subsequent appends are its elements in order.</summary>
        public void StartArray()
        {
            BeforeValue();
            stack.Add(Frame.NewArray(value.Count));
        }

        /// <summary>End the current array.</summary>
        public void EndArray()
        {
            Frame f = Top();
            if (f == null || f.IsObject)
            {
                throw new VariantException("VariantBuilder: EndArray with no matching StartArray");
            }
            stack.RemoveAt(stack.Count - 1);
            FinishWritingArray(f.Start, f.Offsets);
        }

        // --- finalize ---

        /// <summary>Finalize and return the built <see cref="Variant" />.</summary>
        public Variant Build()
        {
            if (stack.Count != 0)
            {
                throw new VariantException("VariantBuilder: Build called with an open container");
            }
            if (!rootWritten)
            {
                throw new VariantException("VariantBuilder: Build called with no value");
            }
            Finish(out byte[] v, out byte[] m);
            return new Variant(v, m);
        }

        // --- JSON bridge (behind Variant.ParseJson) ---

        /// <summary>
        ///     Build (value, metadata) bytes from a JSON string, driving the streaming API. Number
        ///     handling follows Java VariantUtils.fromJsonNode: a fractional JSON number becomes a
        ///     DOUBLE; an integer becomes the smallest int1/2/4/8 that fits, or a scale-0 decimal when
        ///     wider than 64 bits.
        /// </summary>
        internal static void Build(string json, out byte[] value, out byte[] metadata)
        {
            ValidateJsonNumbers(json);
            var builder = new VariantBuilder();
            builder.ProcessJson(JToken.Parse(json));
            builder.Finish(out value, out metadata);
        }

        // Newtonsoft's JToken.Parse accepts non-standard JSON number grammar (leading zeros,
        // leading/trailing decimal points, etc.) that serde_json/Jackson reject. Pre-validate the
        // raw text against the strict RFC 8259 number grammar so ParseJson matches the Java
        // reference. Numbers inside string literals are skipped.
        private static void ValidateJsonNumbers(string json)
        {
            if (json == null) throw new ArgumentNullException(nameof(json));
            bool inString = false;
            int i = 0;
            while (i < json.Length)
            {
                char c = json[i];
                if (inString)
                {
                    if (c == '\\')
                    {
                        i += 2; // skip the escaped char
                        continue;
                    }
                    if (c == '"') inString = false;
                    i++;
                    continue;
                }
                if (c == '"')
                {
                    inString = true;
                    i++;
                    continue;
                }
                if (c == '-' || c == '+' || c == '.' || (c >= '0' && c <= '9'))
                {
                    // Permit the non-finite bareword -Infinity, which Newtonsoft accepts (as
                    // Jackson does with ALLOW_NON_NUMERIC_NUMBERS) and which parses to
                    // double.NegativeInfinity. It is the only non-finite literal whose leading
                    // char ('-') triggers the strict number scan; NaN and Infinity begin with
                    // letters and are skipped past without validation. Handling it here keeps the
                    // RFC 8259 rejection of 007/.5/1. intact.
                    if (c == '-' && MatchesAt(json, i, "-Infinity"))
                    {
                        i += 9; // length of "-Infinity"
                        continue;
                    }
                    int start = i;
                    while (i < json.Length && IsNumberChar(json[i])) i++;
                    string token = json.Substring(start, i - start);
                    if (!IsValidJsonNumber(token))
                    {
                        throw new VariantException(
                            $"malformed variant: invalid JSON number '{token}'");
                    }
                    continue;
                }
                i++;
            }
        }

        private static bool IsNumberChar(char c)
        {
            return c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E'
                   || (c >= '0' && c <= '9');
        }

        // Whether json contains literal at position i.
        private static bool MatchesAt(string json, int i, string literal)
        {
            if (i + literal.Length > json.Length) return false;
            for (int k = 0; k < literal.Length; k++)
            {
                if (json[i + k] != literal[k]) return false;
            }
            return true;
        }

        // Validate a captured number token against the RFC 8259 number grammar.
        private static bool IsValidJsonNumber(string token)
        {
            int len = token.Length;
            int i = 0;
            if (i < len && token[i] == '-') i++;
            // integer part
            if (i >= len) return false;
            if (token[i] == '0')
            {
                i++;
            }
            else if (token[i] >= '1' && token[i] <= '9')
            {
                i++;
                while (i < len && token[i] >= '0' && token[i] <= '9') i++;
            }
            else
            {
                return false;
            }
            // fraction
            if (i < len && token[i] == '.')
            {
                i++;
                if (i >= len || token[i] < '0' || token[i] > '9') return false;
                while (i < len && token[i] >= '0' && token[i] <= '9') i++;
            }
            // exponent
            if (i < len && (token[i] == 'e' || token[i] == 'E'))
            {
                i++;
                if (i < len && (token[i] == '+' || token[i] == '-')) i++;
                if (i >= len || token[i] < '0' || token[i] > '9') return false;
                while (i < len && token[i] >= '0' && token[i] <= '9') i++;
            }
            return i == len;
        }

        private void ProcessJson(JToken token)
        {
            switch (token.Type)
            {
                case JTokenType.Object:
                {
                    StartObject();
                    foreach (JProperty prop in ((JObject)token).Properties())
                    {
                        AppendKey(prop.Name);
                        ProcessJson(prop.Value);
                    }
                    EndObject();
                    break;
                }
                case JTokenType.Array:
                {
                    StartArray();
                    foreach (JToken el in (JArray)token) ProcessJson(el);
                    EndArray();
                    break;
                }
                case JTokenType.String:
                    AppendString((string)((JValue)token).Value);
                    break;
                case JTokenType.Boolean:
                    AppendBoolean((bool)((JValue)token).Value);
                    break;
                case JTokenType.Integer:
                {
                    object raw = ((JValue)token).Value;
                    if (raw is BigInteger bi)
                    {
                        if (bi >= long.MinValue && bi <= long.MaxValue) AppendAutoInt((long)bi);
                        else AppendDecimal(bi, 0);
                    }
                    else
                    {
                        AppendAutoInt(Convert.ToInt64(raw, CultureInfo.InvariantCulture));
                    }
                    break;
                }
                case JTokenType.Float:
                    AppendDouble(Convert.ToDouble(((JValue)token).Value, CultureInfo.InvariantCulture));
                    break;
                case JTokenType.Null:
                case JTokenType.Undefined:
                    AppendNull();
                    break;
                default:
                    throw new VariantException("unsupported JSON value: " + token.Type);
            }
        }

        // Smallest-width integer (the JSON classification), used only by the JSON path.
        private void AppendAutoInt(long i)
        {
            BeforeValue();
            if (i >= sbyte.MinValue && i <= sbyte.MaxValue)
            {
                value.Add(PrimitiveHeader(Variant.TInt1));
                AppendLongLE(value, i, 1);
            }
            else if (i >= short.MinValue && i <= short.MaxValue)
            {
                value.Add(PrimitiveHeader(Variant.TInt2));
                AppendLongLE(value, i, 2);
            }
            else if (i >= int.MinValue && i <= int.MaxValue)
            {
                value.Add(PrimitiveHeader(Variant.TInt4));
                AppendLongLE(value, i, 4);
            }
            else
            {
                value.Add(PrimitiveHeader(Variant.TInt8));
                AppendLongLE(value, i, 8);
            }
        }

        // --- nesting-stack bookkeeping ---

        private Frame Top() => stack.Count == 0 ? null : stack[stack.Count - 1];

        // Register the slot the value about to be written occupies in its parent container (or the
        // root). Must be called before any value bytes are emitted.
        private void BeforeValue()
        {
            Frame f = Top();
            if (f == null)
            {
                if (rootWritten)
                {
                    throw new VariantException("VariantBuilder: multiple root values");
                }
                rootWritten = true;
                return;
            }
            if (f.IsObject)
            {
                if (!f.HasPendingKey)
                {
                    throw new VariantException(
                        "VariantBuilder: value appended without a preceding AppendKey");
                }
                f.Fields.Add(new FieldEntry(f.PendingKey, f.PendingId, value.Count - f.Start));
                f.HasPendingKey = false;
            }
            else
            {
                f.Offsets.Add(value.Count - f.Start);
            }
        }

        // --- shared value-emitting helpers (also feed the JSON path) ---

        private void WriteDouble(double d)
        {
            value.Add(PrimitiveHeader(Variant.TDouble));
            byte[] b = BitConverter.GetBytes(d);
            if (!BitConverter.IsLittleEndian) Array.Reverse(b);
            value.AddRange(b);
        }

        private void WriteString(string s)
        {
            byte[] text = Encoding.UTF8.GetBytes(s);
            if (text.Length > Variant.MaxShortStrSize)
            {
                value.Add(PrimitiveHeader(Variant.TLongStr));
                AppendUintLE(value, text.Length, Variant.U32Size);
            }
            else
            {
                value.Add((byte)((text.Length << 2) | Variant.ShortStr));
            }
            value.AddRange(text);
        }

        private void WriteDecimal(BigInteger unscaled, int scale)
        {
            if (scale < 0) throw new VariantException("decimal scale must be non-negative");
            // Rendering to decimal digits is quadratic in .NET - a 300,000-digit coefficient takes
            // 1.4s - and a caller can hand one in. 17 bytes of two's complement is the widest a
            // 38-digit value can be, so anything past that is out of range without rendering it.
            if (unscaled.ToByteArray().Length > 17)
            {
                throw new VariantException("decimal exceeds maximum precision (38)");
            }

            int digits = BigInteger.Abs(unscaled).ToString(CultureInfo.InvariantCulture).Length;
            int code, width;
            if (scale <= 9 && digits <= 9) { code = Variant.TDecimal4; width = 4; }
            else if (scale <= 18 && digits <= 18) { code = Variant.TDecimal8; width = 8; }
            else if (scale <= 38 && digits <= 38) { code = Variant.TDecimal16; width = 16; }
            else throw new VariantException("decimal exceeds maximum precision (38)");
            value.Add(PrimitiveHeader(code));
            value.Add((byte)scale);
            AppendBigIntLE(value, unscaled, width);
        }

        private int AddKey(string key)
        {
            if (dictionary.TryGetValue(key, out int existing)) return existing;
            int id = dictionaryKeys.Count;
            dictionary[key] = id;
            dictionaryKeys.Add(Encoding.UTF8.GetBytes(key));
            return id;
        }

        private void Finish(out byte[] valueOut, out byte[] metadataOut)
        {
            int numKeys = dictionaryKeys.Count;
            int dictStringSize = 0;
            foreach (var k in dictionaryKeys) dictStringSize += k.Length;
            int offsetSize = IntegerSize(Math.Max(dictStringSize, numKeys));

            var metadata = new List<byte>();
            metadata.Add((byte)(Variant.Version | ((offsetSize - 1) << 6)));
            AppendUintLE(metadata, numKeys, offsetSize);
            int currentOffset = 0;
            foreach (var k in dictionaryKeys)
            {
                AppendUintLE(metadata, currentOffset, offsetSize);
                currentOffset += k.Length;
            }
            AppendUintLE(metadata, currentOffset, offsetSize);
            foreach (var k in dictionaryKeys) metadata.AddRange(k);

            valueOut = value.ToArray();
            metadataOut = metadata.ToArray();
        }

        private void FinishWritingArray(int start, List<int> offsets)
        {
            int dataSize = value.Count - start;
            int numOffsets = offsets.Count;
            bool largeSize = numOffsets > 0xFF;
            int sizeBytes = largeSize ? Variant.U32Size : 1;
            int offsetSize = IntegerSize(dataSize);
            var header = new List<byte>
            {
                (byte)(((largeSize ? 1 : 0) << (Variant.BasicTypeBits + 2))
                       | ((offsetSize - 1) << Variant.BasicTypeBits) | Variant.ArrayType),
            };
            AppendUintLE(header, numOffsets, sizeBytes);
            foreach (int offset in offsets) AppendUintLE(header, offset, offsetSize);
            AppendUintLE(header, dataSize, offsetSize);
            value.InsertRange(start, header);
        }

        private void FinishWritingObject(int start, List<FieldEntry> fields)
        {
            int numFields = fields.Count;
            // Compare the already-encoded dictionary key bytes rather than re-encoding on each
            // comparison.
            fields.Sort((a, b) => Variant.CompareKeys(dictionaryKeys[a.Id], dictionaryKeys[b.Id]));
            int maxId = 0;
            foreach (var f in fields) maxId = Math.Max(maxId, f.Id);
            int dataSize = value.Count - start;
            bool largeSize = numFields > 0xFF;
            int sizeBytes = largeSize ? Variant.U32Size : 1;
            int idSize = IntegerSize(maxId);
            int offsetSize = IntegerSize(dataSize);
            var header = new List<byte>
            {
                (byte)(((largeSize ? 1 : 0) << (Variant.BasicTypeBits + 4))
                       | ((idSize - 1) << (Variant.BasicTypeBits + 2))
                       | ((offsetSize - 1) << Variant.BasicTypeBits) | Variant.ObjectType),
            };
            AppendUintLE(header, numFields, sizeBytes);
            foreach (var f in fields) AppendUintLE(header, f.Id, idSize);
            foreach (var f in fields) AppendUintLE(header, f.Offset, offsetSize);
            AppendUintLE(header, dataSize, offsetSize);
            value.InsertRange(start, header);
        }

        // --- low-level byte helpers ---

        private static byte PrimitiveHeader(int typeCode) => (byte)((typeCode << 2) | Variant.Primitive);

        private static int IntegerSize(int v)
        {
            if (v <= 0xFF) return 1;
            if (v <= 0xFFFF) return 2;
            if (v <= 0xFFFFFF) return 3;
            return 4;
        }

        private static void AppendUintLE(List<byte> outBytes, int v, int numBytes)
        {
            for (int i = 0; i < numBytes; i++)
            {
                outBytes.Add((byte)((v >> (8 * i)) & 0xFF));
            }
        }

        private static void AppendLongLE(List<byte> outBytes, long v, int width)
        {
            for (int i = 0; i < width; i++)
            {
                outBytes.Add((byte)(v & 0xFF));
                v >>= 8;
            }
        }

        private static void AppendBigIntLE(List<byte> outBytes, BigInteger n, int width)
        {
            byte[] le = n.ToByteArray(); // little-endian two's-complement, minimal length
            byte pad = (byte)(n.Sign < 0 ? 0xFF : 0x00);
            for (int i = 0; i < width; i++)
            {
                outBytes.Add(i < le.Length ? le[i] : pad);
            }
        }

        // Two's-complement big-endian bytes -> BigInteger.
        private static BigInteger BigIntegerFromBigEndian(byte[] be)
        {
            var le = new byte[be.Length];
            for (int i = 0; i < be.Length; i++) le[i] = be[be.Length - 1 - i];
            return new BigInteger(le);
        }

        private readonly struct FieldEntry
        {
            public FieldEntry(string key, int id, int offset)
            {
                Key = key;
                Id = id;
                Offset = offset;
            }

            public string Key { get; }
            public int Id { get; }
            public int Offset { get; }
        }

        // A pending container on the nesting stack.
        private sealed class Frame
        {
            public bool IsObject;
            public int Start;
            public List<int> Offsets;         // arrays
            public List<FieldEntry> Fields;   // objects
            public bool HasPendingKey;
            public string PendingKey;
            public int PendingId;

            public static Frame NewObject(int start) => new Frame
            {
                IsObject = true,
                Start = start,
                Fields = new List<FieldEntry>(),
            };

            public static Frame NewArray(int start) => new Frame
            {
                IsObject = false,
                Start = start,
                Offsets = new List<int>(),
            };
        }
    }
}
