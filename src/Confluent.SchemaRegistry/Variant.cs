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
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     The value type of a <see cref="Variant" />, mirroring Java's Variant.Type. Integer,
    ///     decimal, and timestamp widths are kept distinct here; the CEL layer collapses them.
    /// </summary>
    public enum VariantType
    {
        Object, Array, Null, Boolean, Byte, Short, Int, Long, String, Double,
        Decimal4, Decimal8, Decimal16, Date, TimestampTz, TimestampNtz, Float, Binary,
        Time, TimestampNanosTz, TimestampNanosNtz, Uuid
    }

    /// <summary>Raised for a malformed or unsupported Variant binary value.</summary>
    public class VariantException : Exception
    {
        public VariantException(string message) : base(message) { }
    }

    /// <summary>
    ///     A read-only view over a Variant (a Spark/Parquet Variant: a metadata key-dictionary
    ///     plus a self-describing value stream) at a byte position. Navigation
    ///     (<see cref="GetFieldByKey" /> / <see cref="GetElementAtIndex" />) returns a
    ///     sub-<see cref="Variant" /> sharing the same buffers. This is the .NET counterpart of
    ///     Java's <c>io.confluent.kafka.schemaregistry.type.Variant</c>; it lives in the core
    ///     package so the Avro and Protobuf serdes can use it without a Rules dependency.
    ///
    ///     <para><see cref="ToJson" /> renders temporal types as ISO-8601 with the seconds field
    ///     always present (0/3/6/9-digit fractional grouping) and decimals in fixed-point - the
    ///     cross-language contract. <see cref="ParseJson" /> follows Java number handling: a
    ///     fractional JSON number becomes a DOUBLE, an integer wider than 64 bits a scale-0
    ///     decimal.</para>
    /// </summary>
    public sealed class Variant
    {
        // Basic types (low 2 bits of the header byte).
        internal const int Primitive = 0;
        internal const int ShortStr = 1;
        internal const int ObjectType = 2;
        internal const int ArrayType = 3;

        // Primitive type codes (upper 6 bits when basic type == Primitive).
        internal const int TNull = 0;
        internal const int TTrue = 1;
        internal const int TFalse = 2;
        internal const int TInt1 = 3;
        internal const int TInt2 = 4;
        internal const int TInt4 = 5;
        internal const int TInt8 = 6;
        internal const int TDouble = 7;
        internal const int TDecimal4 = 8;
        internal const int TDecimal8 = 9;
        internal const int TDecimal16 = 10;
        internal const int TDate = 11;
        internal const int TTimestamp = 12;
        internal const int TTimestampNtz = 13;
        internal const int TFloat = 14;
        internal const int TBinary = 15;
        internal const int TLongStr = 16;
        internal const int TTime = 17;
        internal const int TTimestampNanos = 18;
        internal const int TTimestampNanosNtz = 19;
        internal const int TUuid = 20;

        internal const int BasicTypeMask = 0x3;
        internal const int BasicTypeBits = 2;
        internal const int TypeInfoMask = 0x3F;
        internal const int MaxShortStrSize = 0x3F;
        internal const byte Version = 1;
        internal const int VersionMask = 0x0F;
        internal const int U32Size = 4;
        private const int BinarySearchThreshold = 32;

        private readonly byte[] value;
        private readonly byte[] metadata;
        private readonly int pos;

        /// <summary>Construct from raw value + metadata byte arrays.</summary>
        public Variant(byte[] value, byte[] metadata) : this(value, metadata, 0) { }

        internal Variant(byte[] value, byte[] metadata, int pos)
        {
            this.value = value ?? throw new ArgumentNullException(nameof(value));
            this.metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
            this.pos = pos;
            CheckIndex(0, this.metadata.Length);
            if ((this.metadata[0] & VersionMask) != Version)
            {
                throw new VariantException(
                    "unsupported variant metadata version: " + (this.metadata[0] & VersionMask));
            }
        }

        /// <summary>The raw value bytes (the whole buffer, shared across sub-variants).</summary>
        public byte[] ValueBytes => value;

        /// <summary>The raw metadata bytes (the key dictionary).</summary>
        public byte[] MetadataBytes => metadata;

        internal int Position => pos;

        // --- type ---

        public VariantType GetVariantType()
        {
            CheckIndex(pos, value.Length);
            int basicType = value[pos] & BasicTypeMask;
            int typeInfo = (value[pos] >> BasicTypeBits) & TypeInfoMask;
            switch (basicType)
            {
                case ShortStr: return VariantType.String;
                case ObjectType: return VariantType.Object;
                case ArrayType: return VariantType.Array;
            }
            switch (typeInfo)
            {
                case TNull: return VariantType.Null;
                case TTrue:
                case TFalse: return VariantType.Boolean;
                case TInt1: return VariantType.Byte;
                case TInt2: return VariantType.Short;
                case TInt4: return VariantType.Int;
                case TInt8: return VariantType.Long;
                case TDouble: return VariantType.Double;
                case TDecimal4: return VariantType.Decimal4;
                case TDecimal8: return VariantType.Decimal8;
                case TDecimal16: return VariantType.Decimal16;
                case TDate: return VariantType.Date;
                case TTimestamp: return VariantType.TimestampTz;
                case TTimestampNtz: return VariantType.TimestampNtz;
                case TFloat: return VariantType.Float;
                case TBinary: return VariantType.Binary;
                case TLongStr: return VariantType.String;
                case TTime: return VariantType.Time;
                case TTimestampNanos: return VariantType.TimestampNanosTz;
                case TTimestampNanosNtz: return VariantType.TimestampNanosNtz;
                case TUuid: return VariantType.Uuid;
                default:
                    throw new VariantException("unknown variant primitive type: " + typeInfo);
            }
        }

        // --- scalar getters ---

        private int PrimitiveInfo()
        {
            CheckIndex(pos, value.Length);
            int basicType = value[pos] & BasicTypeMask;
            if (basicType != Primitive)
            {
                throw new VariantException("expected a primitive variant value");
            }
            return (value[pos] >> BasicTypeBits) & TypeInfoMask;
        }

        public bool GetBoolean()
        {
            int ti = PrimitiveInfo();
            if (ti != TTrue && ti != TFalse) throw new VariantException("variant is not a boolean");
            return ti == TTrue;
        }

        /// <summary>The signed 8-bit integer of an INT8 value (exact width; no widening).</summary>
        public sbyte GetByte()
        {
            int ti = PrimitiveInfo();
            if (ti != TInt1) throw new VariantException("variant is not a byte");
            return (sbyte)ReadSignedLong(value, pos + 1, 1);
        }

        /// <summary>The signed 16-bit integer of an INT8/INT16 value (widens within 16 bits).</summary>
        public short GetShort()
        {
            int ti = PrimitiveInfo();
            switch (ti)
            {
                case TInt1: return (short)ReadSignedLong(value, pos + 1, 1);
                case TInt2: return (short)ReadSignedLong(value, pos + 1, 2);
                default: throw new VariantException("variant is not a short");
            }
        }

        /// <summary>The signed 32-bit integer of an INT8/INT16/INT32 value (widens within 32 bits).</summary>
        public int GetInt()
        {
            int ti = PrimitiveInfo();
            switch (ti)
            {
                case TInt1: return (int)ReadSignedLong(value, pos + 1, 1);
                case TInt2: return (int)ReadSignedLong(value, pos + 1, 2);
                case TInt4: return (int)ReadSignedLong(value, pos + 1, 4);
                default: throw new VariantException("variant is not an int");
            }
        }

        /// <summary>
        ///     The raw integer for any integer-backed type (byte/short/int/long, date days,
        ///     timestamp micros, time micros, timestamp-nanos) - mirrors Java <c>getLong</c>.
        /// </summary>
        public long GetLong()
        {
            int ti = PrimitiveInfo();
            switch (ti)
            {
                case TInt1: return ReadSignedLong(value, pos + 1, 1);
                case TInt2: return ReadSignedLong(value, pos + 1, 2);
                case TInt4:
                case TDate: return ReadSignedLong(value, pos + 1, 4);
                case TInt8:
                case TTimestamp:
                case TTimestampNtz:
                case TTime:
                case TTimestampNanos:
                case TTimestampNanosNtz: return ReadSignedLong(value, pos + 1, 8);
                default: throw new VariantException("variant is not an integer-backed type");
            }
        }

        /// <summary>The 32-bit float of a FLOAT value (exact; does not read DOUBLE).</summary>
        public float GetFloat()
        {
            int ti = PrimitiveInfo();
            if (ti != TFloat) throw new VariantException("variant is not a float");
            return ReadFloatLE(value, pos + 1);
        }

        /// <summary>The 64-bit double of a DOUBLE value (exact; does not widen FLOAT).</summary>
        public double GetDouble()
        {
            int ti = PrimitiveInfo();
            if (ti == TDouble) return ReadDoubleLE(value, pos + 1);
            throw new VariantException("variant is not a double");
        }

        /// <summary>The unscaled integer and scale of a decimal value (scale preserved).</summary>
        public void GetDecimalParts(out BigInteger unscaled, out int scale)
        {
            int ti = PrimitiveInfo();
            CheckIndex(pos + 1, value.Length);
            scale = value[pos + 1];
            int width;
            if (ti == TDecimal4) width = 4;
            else if (ti == TDecimal8) width = 8;
            else if (ti == TDecimal16) width = 16;
            else throw new VariantException("variant is not a decimal");
            CheckIndex(pos + 2 + width - 1, value.Length);
            var le = new byte[width];
            Array.Copy(value, pos + 2, le, 0, width);
            unscaled = new BigInteger(le); // .NET BigInteger(byte[]) is little-endian two's-complement
        }

        public byte[] GetBinary()
        {
            int ti = PrimitiveInfo();
            if (ti != TBinary) throw new VariantException("variant is not binary");
            int length = ReadUnsignedLE(value, pos + 1, U32Size);
            int start = pos + 1 + U32Size;
            CheckIndex(start + length - 1, value.Length);
            var result = new byte[length];
            Array.Copy(value, start, result, 0, length);
            return result;
        }

        /// <summary>The UUID as its canonical big-endian hex string (e.g. "00112233-...").</summary>
        public string GetUuid()
        {
            int ti = PrimitiveInfo();
            if (ti != TUuid) throw new VariantException("variant is not a uuid");
            int start = pos + 1;
            CheckIndex(start + 15, value.Length);
            return FormatUuid(value, start);
        }

        public string GetString()
        {
            CheckIndex(pos, value.Length);
            int basicType = value[pos] & BasicTypeMask;
            int typeInfo = (value[pos] >> BasicTypeBits) & TypeInfoMask;
            int start, length;
            if (basicType == ShortStr)
            {
                start = pos + 1;
                length = typeInfo;
            }
            else if (basicType == Primitive && typeInfo == TLongStr)
            {
                length = ReadUnsignedLE(value, pos + 1, U32Size);
                start = pos + 1 + U32Size;
            }
            else
            {
                throw new VariantException("variant is not a string");
            }
            CheckIndex(start + length - 1, value.Length);
            return Encoding.UTF8.GetString(value, start, length);
        }

        // --- object / array navigation ---

        private void ObjectInfo(out int numFields, out int idSize, out int offsetSize,
            out int idStart, out int offsetStart, out int dataStart)
        {
            CheckIndex(pos, value.Length);
            int basicType = value[pos] & BasicTypeMask;
            int typeInfo = (value[pos] >> BasicTypeBits) & TypeInfoMask;
            if (basicType != ObjectType) throw new VariantException("variant is not an object");
            bool largeSize = ((typeInfo >> 4) & 0x1) != 0;
            int sizeBytes = largeSize ? U32Size : 1;
            numFields = ReadUnsignedLE(value, pos + 1, sizeBytes);
            idSize = ((typeInfo >> 2) & 0x3) + 1;
            offsetSize = (typeInfo & 0x3) + 1;
            idStart = pos + 1 + sizeBytes;
            offsetStart = idStart + numFields * idSize;
            dataStart = offsetStart + (numFields + 1) * offsetSize;
        }

        private void ArrayInfo(out int numFields, out int offsetSize,
            out int offsetStart, out int dataStart)
        {
            CheckIndex(pos, value.Length);
            int basicType = value[pos] & BasicTypeMask;
            int typeInfo = (value[pos] >> BasicTypeBits) & TypeInfoMask;
            if (basicType != ArrayType) throw new VariantException("variant is not an array");
            bool largeSize = ((typeInfo >> 2) & 0x1) != 0;
            int sizeBytes = largeSize ? U32Size : 1;
            numFields = ReadUnsignedLE(value, pos + 1, sizeBytes);
            offsetSize = (typeInfo & 0x3) + 1;
            offsetStart = pos + 1 + sizeBytes;
            dataStart = offsetStart + (numFields + 1) * offsetSize;
        }

        public int NumObjectFields()
        {
            ObjectInfo(out int n, out _, out _, out _, out _, out _);
            return n;
        }

        public int NumArrayElements()
        {
            ArrayInfo(out int n, out _, out _, out _);
            return n;
        }

        /// <summary>The object field with the given key, or null if absent.</summary>
        public Variant GetFieldByKey(string key)
        {
            ObjectInfo(out int numFields, out int idSize, out int offsetSize,
                out int idStart, out int offsetStart, out int dataStart);
            if (numFields < BinarySearchThreshold)
            {
                for (int i = 0; i < numFields; i++)
                {
                    int id = ReadUnsignedLE(value, idStart + idSize * i, idSize);
                    if (GetMetadataKey(id) == key)
                    {
                        int offset = ReadUnsignedLE(value, offsetStart + offsetSize * i, offsetSize);
                        return new Variant(value, metadata, dataStart + offset);
                    }
                }
                return null;
            }
            // Encode the lookup key once, outside the loop, rather than on every comparison.
            byte[] keyBytes = EncodeKey(key);
            int low = 0, high = numFields - 1;
            while (low <= high)
            {
                int mid = (low + high) >> 1;
                int midId = ReadUnsignedLE(value, idStart + idSize * mid, idSize);
                int cmp = CompareKeys(EncodeKey(GetMetadataKey(midId)), keyBytes);
                if (cmp < 0) low = mid + 1;
                else if (cmp > 0) high = mid - 1;
                else
                {
                    int offset = ReadUnsignedLE(value, offsetStart + offsetSize * mid, offsetSize);
                    return new Variant(value, metadata, dataStart + offset);
                }
            }
            return null;
        }

        /// <summary>
        ///     Encodes an object field key to the UTF-8 bytes that <see cref="CompareKeys" />
        ///     orders. Callers that compare the same key repeatedly should encode it once.
        /// </summary>
        internal static byte[] EncodeKey(string key)
        {
            return Encoding.UTF8.GetBytes(key);
        }

        /// <summary>
        ///     Compares two object field keys by UTF-8 byte order, as required by the Variant
        ///     spec. This differs from ordinal (UTF-16 code unit) comparison for supplementary
        ///     characters (U+10000 and above), whose surrogate code units sort before U+E000-U+FFFF
        ///     in UTF-16 but after them in UTF-8.
        /// </summary>
        internal static int CompareKeys(byte[] a, byte[] b)
        {
            return a.AsSpan().SequenceCompareTo(b);
        }

        /// <summary>The (key, value) of the field at <paramref name="idx" /> (key-sorted).</summary>
        public KeyValuePair<string, Variant> GetFieldAtIndex(int idx)
        {
            ObjectInfo(out _, out int idSize, out int offsetSize,
                out int idStart, out int offsetStart, out int dataStart);
            int id = ReadUnsignedLE(value, idStart + idSize * idx, idSize);
            int offset = ReadUnsignedLE(value, offsetStart + offsetSize * idx, offsetSize);
            return new KeyValuePair<string, Variant>(
                GetMetadataKey(id), new Variant(value, metadata, dataStart + offset));
        }

        /// <summary>The array element at <paramref name="index" />, or null if out of bounds.</summary>
        public Variant GetElementAtIndex(int index)
        {
            ArrayInfo(out int numFields, out int offsetSize, out int offsetStart, out int dataStart);
            if (index < 0 || index >= numFields) return null;
            int offset = ReadUnsignedLE(value, offsetStart + offsetSize * index, offsetSize);
            return new Variant(value, metadata, dataStart + offset);
        }

        // --- JSON ---

        /// <summary>Serialize to a JSON string, matching Java's VariantUtils.toJsonString.</summary>
        public string ToJson()
        {
            var sb = new StringBuilder();
            WriteJson(sb);
            return sb.ToString();
        }

        private void WriteJson(StringBuilder sb)
        {
            VariantType t = GetVariantType();
            switch (t)
            {
                case VariantType.Object:
                {
                    sb.Append('{');
                    int n = NumObjectFields();
                    for (int i = 0; i < n; i++)
                    {
                        if (i > 0) sb.Append(',');
                        var field = GetFieldAtIndex(i);
                        sb.Append(JsonConvert.ToString(field.Key)).Append(':');
                        field.Value.WriteJson(sb);
                    }
                    sb.Append('}');
                    break;
                }
                case VariantType.Array:
                {
                    sb.Append('[');
                    int n = NumArrayElements();
                    for (int i = 0; i < n; i++)
                    {
                        if (i > 0) sb.Append(',');
                        GetElementAtIndex(i).WriteJson(sb);
                    }
                    sb.Append(']');
                    break;
                }
                case VariantType.Null: sb.Append("null"); break;
                case VariantType.Boolean: sb.Append(GetBoolean() ? "true" : "false"); break;
                case VariantType.String: sb.Append(JsonConvert.ToString(GetString())); break;
                case VariantType.Byte:
                case VariantType.Short:
                case VariantType.Int:
                case VariantType.Long:
                    sb.Append(GetLong().ToString(CultureInfo.InvariantCulture));
                    break;
                case VariantType.Float:
                    sb.Append(FormatFloat(GetFloat()));
                    break;
                case VariantType.Double:
                    sb.Append(FormatDouble(GetDouble()));
                    break;
                case VariantType.Decimal4:
                case VariantType.Decimal8:
                case VariantType.Decimal16:
                {
                    GetDecimalParts(out BigInteger unscaled, out int scale);
                    sb.Append(DecimalPlainString(unscaled, scale));
                    break;
                }
                case VariantType.Date:
                    sb.Append('"').Append(FormatDate(GetLong())).Append('"');
                    break;
                case VariantType.TimestampTz:
                    sb.Append('"').Append(FormatInstant(GetLong() * 1000L)).Append('"');
                    break;
                case VariantType.TimestampNtz:
                    sb.Append('"').Append(FormatLocalDateTime(GetLong() * 1000L)).Append('"');
                    break;
                case VariantType.TimestampNanosTz:
                    sb.Append('"').Append(FormatInstant(GetLong())).Append('"');
                    break;
                case VariantType.TimestampNanosNtz:
                    sb.Append('"').Append(FormatLocalDateTime(GetLong())).Append('"');
                    break;
                case VariantType.Time:
                    sb.Append('"').Append(FormatLocalTime(GetLong())).Append('"');
                    break;
                case VariantType.Binary:
                    sb.Append('"').Append(Convert.ToBase64String(GetBinary())).Append('"');
                    break;
                case VariantType.Uuid:
                    sb.Append('"').Append(GetUuid()).Append('"');
                    break;
                default:
                    throw new VariantException("unsupported variant type for JSON: " + t);
            }
        }

        // --- metadata dictionary ---

        private string GetMetadataKey(int id)
        {
            CheckIndex(0, metadata.Length);
            int offsetSize = ((metadata[0] >> 6) & 0x3) + 1;
            int dictSize = ReadUnsignedLE(metadata, 1, offsetSize);
            if (id >= dictSize) throw new VariantException("malformed variant: field id out of range");
            int stringStart = 1 + (dictSize + 2) * offsetSize;
            int offset = ReadUnsignedLE(metadata, 1 + (id + 1) * offsetSize, offsetSize);
            int nextOffset = ReadUnsignedLE(metadata, 1 + (id + 2) * offsetSize, offsetSize);
            if (offset > nextOffset)
            {
                throw new VariantException("malformed variant: non-monotonic metadata offsets");
            }
            CheckIndex(stringStart + nextOffset - 1, metadata.Length);
            return Encoding.UTF8.GetString(metadata, stringStart + offset, nextOffset - offset);
        }

        // --- low-level byte helpers ---

        private static void CheckIndex(int pos, int length)
        {
            if (pos < 0 || pos >= length)
            {
                throw new VariantException("malformed variant: index out of bounds");
            }
        }

        private static int ReadUnsignedLE(byte[] data, int pos, int numBytes)
        {
            CheckIndex(pos, data.Length);
            CheckIndex(pos + numBytes - 1, data.Length);
            int result = 0;
            for (int i = numBytes - 1; i >= 0; i--)
            {
                result = (result << 8) | data[pos + i];
            }
            return result;
        }

        private static long ReadSignedLong(byte[] data, int pos, int numBytes)
        {
            CheckIndex(pos, data.Length);
            CheckIndex(pos + numBytes - 1, data.Length);
            ulong result = 0;
            for (int i = numBytes - 1; i >= 0; i--)
            {
                result = (result << 8) | data[pos + i];
            }
            if (numBytes < 8)
            {
                ulong signBit = 1UL << (numBytes * 8 - 1);
                if ((result & signBit) != 0)
                {
                    result |= ulong.MaxValue << (numBytes * 8);
                }
            }
            return unchecked((long)result);
        }

        private static float ReadFloatLE(byte[] data, int pos)
        {
            CheckIndex(pos + 3, data.Length);
            var b = new byte[4];
            Array.Copy(data, pos, b, 0, 4);
            if (!BitConverter.IsLittleEndian) Array.Reverse(b);
            return BitConverter.ToSingle(b, 0);
        }

        private static double ReadDoubleLE(byte[] data, int pos)
        {
            CheckIndex(pos + 7, data.Length);
            var b = new byte[8];
            Array.Copy(data, pos, b, 0, 8);
            if (!BitConverter.IsLittleEndian) Array.Reverse(b);
            return BitConverter.ToDouble(b, 0);
        }

        // --- formatting (cross-language contract) ---

        private static long FloorDiv(long a, long b)
        {
            long q = a / b;
            if ((a % b != 0) && ((a < 0) != (b < 0))) q--;
            return q;
        }

        private static long FloorMod(long a, long b)
        {
            long r = a % b;
            if (r != 0 && ((r < 0) != (b < 0))) r += b;
            return r;
        }

        private static string Frac(long nano)
        {
            if (nano == 0) return "";
            if (nano % 1_000_000 == 0) return "." + (nano / 1_000_000).ToString("D3", CultureInfo.InvariantCulture);
            if (nano % 1_000 == 0) return "." + (nano / 1_000).ToString("D6", CultureInfo.InvariantCulture);
            return "." + nano.ToString("D9", CultureInfo.InvariantCulture);
        }

        private static string FormatInstant(long totalNanos)
        {
            long sec = FloorDiv(totalNanos, 1_000_000_000L);
            long nano = FloorMod(totalNanos, 1_000_000_000L);
            DateTime dt = DateTimeOffset.FromUnixTimeSeconds(sec).UtcDateTime;
            return string.Format(CultureInfo.InvariantCulture,
                "{0:D4}-{1:D2}-{2:D2}T{3:D2}:{4:D2}:{5:D2}{6}Z",
                dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, Frac(nano));
        }

        private static string FormatLocalDateTime(long totalNanos)
        {
            long sec = FloorDiv(totalNanos, 1_000_000_000L);
            long nano = FloorMod(totalNanos, 1_000_000_000L);
            DateTime dt = DateTimeOffset.FromUnixTimeSeconds(sec).UtcDateTime;
            return string.Format(CultureInfo.InvariantCulture,
                "{0:D4}-{1:D2}-{2:D2}T{3:D2}:{4:D2}:{5:D2}{6}",
                dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, Frac(nano));
        }

        private static string FormatLocalTime(long micros)
        {
            long nanoOfDay = micros * 1000L;
            long secs = FloorDiv(nanoOfDay, 1_000_000_000L);
            long nano = FloorMod(nanoOfDay, 1_000_000_000L);
            long hour = secs / 3600;
            long rem = secs % 3600;
            return string.Format(CultureInfo.InvariantCulture,
                "{0:D2}:{1:D2}:{2:D2}{3}", hour, rem / 60, rem % 60, Frac(nano));
        }

        private static string FormatDate(long days)
        {
            DateTime dt = DateTimeOffset.FromUnixTimeSeconds(days * 86400L).UtcDateTime;
            return string.Format(CultureInfo.InvariantCulture,
                "{0:D4}-{1:D2}-{2:D2}", dt.Year, dt.Month, dt.Day);
        }

        // Exact fixed-point string for unscaled*10^-scale (never scientific), toPlainString-style.
        internal static string DecimalPlainString(BigInteger unscaled, int scale)
        {
            bool negative = unscaled.Sign < 0;
            string digits = BigInteger.Abs(unscaled).ToString(CultureInfo.InvariantCulture);
            string sign = negative ? "-" : "";
            if (scale == 0) return sign + digits;
            if (digits.Length <= scale)
            {
                digits = new string('0', scale - digits.Length + 1) + digits;
            }
            int point = digits.Length - scale;
            return sign + digits.Substring(0, point) + "." + digits.Substring(point);
        }

        // Integral doubles render as N.0; other values use the shortest round-trip. (Java's
        // Double.toString scientific-notation edge cases are a known minor divergence.)
        private static string FormatDouble(double d)
        {
            if (double.IsNaN(d) || double.IsInfinity(d))
            {
                throw new VariantException("cannot render non-finite double as JSON");
            }
            if (d == Math.Floor(d) && Math.Abs(d) < 1e16)
            {
                return ((long)d).ToString(CultureInfo.InvariantCulture) + ".0";
            }
            return d.ToString("R", CultureInfo.InvariantCulture);
        }

        // Integral floats render as N.0; other values use the shortest decimal that
        // round-trips to the same float32 (matches Java Float.toString / Apache Arrow).
        // A robust shortest-round-trip search is used because float "R"/"G9" is not
        // reliably shortest across all target frameworks (e.g. net462).
        private static string FormatFloat(float f)
        {
            if (float.IsNaN(f) || float.IsInfinity(f))
            {
                throw new VariantException("cannot render non-finite float as JSON");
            }
            if (f == Math.Floor(f) && Math.Abs(f) < 1e16f)
            {
                return ((long)f).ToString(CultureInfo.InvariantCulture) + ".0";
            }
            for (int p = 1; p <= 9; p++)
            {
                string s = f.ToString("G" + p, CultureInfo.InvariantCulture);
                if (float.Parse(s, CultureInfo.InvariantCulture) == f) return s;
            }
            return f.ToString("G9", CultureInfo.InvariantCulture);
        }

        private static string FormatUuid(byte[] data, int start)
        {
            var sb = new StringBuilder(36);
            for (int i = 0; i < 16; i++)
            {
                if (i == 4 || i == 6 || i == 8 || i == 10) sb.Append('-');
                sb.Append(data[start + i].ToString("x2", CultureInfo.InvariantCulture));
            }
            return sb.ToString();
        }

        // --- module-level convenience API ---

        /// <summary>Parse a JSON string into a Variant (matches Java VariantUtils.fromJsonNode).</summary>
        public static Variant ParseJson(string json)
        {
            VariantBuilder.Build(json, out byte[] value, out byte[] metadata);
            return new Variant(value, metadata);
        }
    }
}
