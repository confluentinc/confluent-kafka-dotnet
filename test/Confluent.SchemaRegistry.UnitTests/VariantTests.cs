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
    /// <summary>Tests for the Variant binary codec (reader, builder, and JSON conversion).</summary>
    public class VariantTests
    {
        // Primitive type codes (see Variant).
        private const int TNull = 0, TTrue = 1, TFalse = 2, TInt1 = 3, TInt2 = 4, TInt4 = 5,
            TInt8 = 6, TDouble = 7, TDecimal4 = 8, TDecimal8 = 9, TDecimal16 = 10, TDate = 11,
            TTimestamp = 12, TTimestampNtz = 13, TFloat = 14, TBinary = 15, TTime = 17,
            TTimestampNanos = 18, TUuid = 20;

        // Minimal empty metadata: version 1, offset_size 1, dictionary_size 0.
        private static readonly byte[] EmptyMeta = { 1, 0, 0 };

        private static Variant Prim(int code, params byte[] payload)
        {
            var value = new byte[1 + payload.Length];
            value[0] = (byte)(code << 2);
            Array.Copy(payload, 0, value, 1, payload.Length);
            return new Variant(value, EmptyMeta);
        }

        private static byte[] Le(long value, int width)
        {
            var b = new byte[width];
            for (int i = 0; i < width; i++) { b[i] = (byte)(value & 0xFF); value >>= 8; }
            return b;
        }

        private static byte[] F64(double d)
        {
            var b = BitConverter.GetBytes(d);
            if (!BitConverter.IsLittleEndian) Array.Reverse(b);
            return b;
        }

        private static byte[] F32(float f)
        {
            var b = BitConverter.GetBytes(f);
            if (!BitConverter.IsLittleEndian) Array.Reverse(b);
            return b;
        }

        private static byte[] Dec(int scale, BigInteger unscaled, int width)
        {
            var le = unscaled.ToByteArray();
            byte pad = (byte)(unscaled.Sign < 0 ? 0xFF : 0x00);
            var payload = new byte[1 + width];
            payload[0] = (byte)scale;
            for (int i = 0; i < width; i++) payload[1 + i] = i < le.Length ? le[i] : pad;
            return payload;
        }

        [Fact]
        public void ParseJson_NavigatesAndReadsScalars()
        {
            Variant v = Variant.ParseJson(
                "{\"name\":\"alice\",\"age\":30,\"scores\":[10,20,30],\"nested\":{\"x\":1},\"explicit\":null}");
            Assert.Equal(VariantType.Object, v.GetVariantType());
            Assert.Equal(5, v.NumObjectFields());
            Assert.Equal("alice", v.GetFieldByKey("name").GetString());
            Assert.Equal(30L, v.GetFieldByKey("age").GetLong());
            Assert.Equal(30L, v.GetFieldByKey("scores").GetElementAtIndex(2).GetLong());
            Assert.Equal(3, v.GetFieldByKey("scores").NumArrayElements());
            // Absent (missing) vs present-but-variant-null (explicit JSON null).
            Assert.Null(v.GetFieldByKey("missing"));
            Assert.Equal(VariantType.Null, v.GetFieldByKey("explicit").GetVariantType());
        }

        [Fact]
        public void GetFieldByKey_UsesBinarySearchPastThreshold()
        {
            var sb = new System.Text.StringBuilder("{");
            for (int i = 0; i < 40; i++)
            {
                if (i > 0) sb.Append(',');
                sb.Append('"').Append("k").Append(i.ToString("D2")).Append("\":").Append(i);
            }
            sb.Append('}');
            Variant v = Variant.ParseJson(sb.ToString());
            Assert.Equal(39L, v.GetFieldByKey("k39").GetLong());
            Assert.Equal(0L, v.GetFieldByKey("k00").GetLong());
            Assert.Null(v.GetFieldByKey("k40"));
        }

        [Fact]
        public void ObjectKeysSortedByUtf8ByteOrder()
        {
            // Object field keys must be ordered by the unsigned byte order of their UTF-8
            // encoding, not by UTF-16/ordinal order. U+FFFF encodes to UTF-8 EF BF BF and
            // U+10000 to F0 90 80 80, so U+FFFF must sort first - but in UTF-16 the high
            // surrogate 0xD800 of U+10000 sorts before 0xFFFF. Append in the wrong order.
            string bmpKey = "￿";                   // U+FFFF
            string supplementaryKey = "\U00010000";     // U+10000 (surrogate pair)

            var b = new VariantBuilder();
            b.StartObject();
            b.AppendKey(supplementaryKey); b.AppendLong(2);
            b.AppendKey(bmpKey); b.AppendLong(1);
            b.EndObject();
            Variant v = b.Build();

            Assert.Equal(VariantType.Object, v.GetVariantType());
            Assert.Equal(2, v.NumObjectFields());
            Assert.Equal(bmpKey, v.GetFieldAtIndex(0).Key);
            Assert.Equal(supplementaryKey, v.GetFieldAtIndex(1).Key);
            Assert.Equal(1L, v.GetFieldByKey(bmpKey).GetLong());
            Assert.Equal(2L, v.GetFieldByKey(supplementaryKey).GetLong());
        }

        [Fact]
        public void LargeObjectBinarySearchWithSupplementaryKey()
        {
            // 42 fields exceeds the binary-search threshold (32), so the reader's binary
            // search runs; it must compare keys by UTF-8 byte order for the supplementary key.
            string bmpKey = "￿";                   // U+FFFF
            string supplementaryKey = "\U00010000";     // U+10000 (surrogate pair)

            var b = new VariantBuilder();
            b.StartObject();
            for (int i = 0; i < 40; i++)
            {
                b.AppendKey($"a{i:D3}");
                b.AppendLong(i);
            }
            b.AppendKey(bmpKey); b.AppendLong(998);
            b.AppendKey(supplementaryKey); b.AppendLong(999);
            b.EndObject();
            Variant v = b.Build();

            Assert.Equal(42, v.NumObjectFields());
            Assert.NotNull(v.GetFieldByKey(bmpKey));
            Assert.Equal(998L, v.GetFieldByKey(bmpKey).GetLong());
            Assert.NotNull(v.GetFieldByKey(supplementaryKey));
            Assert.Equal(999L, v.GetFieldByKey(supplementaryKey).GetLong());
            Assert.Equal(37L, v.GetFieldByKey("a037").GetLong());
        }

        [Fact]
        public void GetVariantType_ForEachPrimitive()
        {
            Assert.Equal(VariantType.Null, Prim(TNull).GetVariantType());
            Assert.Equal(VariantType.Boolean, Prim(TTrue).GetVariantType());
            Assert.Equal(VariantType.Boolean, Prim(TFalse).GetVariantType());
            Assert.Equal(VariantType.Byte, Prim(TInt1, Le(1, 1)).GetVariantType());
            Assert.Equal(VariantType.Short, Prim(TInt2, Le(1, 2)).GetVariantType());
            Assert.Equal(VariantType.Int, Prim(TInt4, Le(1, 4)).GetVariantType());
            Assert.Equal(VariantType.Long, Prim(TInt8, Le(1, 8)).GetVariantType());
            Assert.Equal(VariantType.Double, Prim(TDouble, F64(1.0)).GetVariantType());
            Assert.Equal(VariantType.Decimal4, Prim(TDecimal4, Dec(2, 1234, 4)).GetVariantType());
            Assert.Equal(VariantType.Decimal16, Prim(TDecimal16, Dec(2, 1234, 16)).GetVariantType());
            Assert.Equal(VariantType.Date, Prim(TDate, Le(18262, 4)).GetVariantType());
            Assert.Equal(VariantType.TimestampTz, Prim(TTimestamp, Le(0, 8)).GetVariantType());
            Assert.Equal(VariantType.Time, Prim(TTime, Le(0, 8)).GetVariantType());
            Assert.Equal(VariantType.TimestampNanosTz, Prim(TTimestampNanos, Le(0, 8)).GetVariantType());
            Assert.Equal(VariantType.Binary, Prim(TBinary, Le(0, 4)).GetVariantType());
            Assert.Equal(VariantType.Uuid, Prim(TUuid, new byte[16]).GetVariantType());
            Assert.Equal(VariantType.String, Variant.ParseJson("\"hi\"").GetVariantType());
        }

        [Fact]
        public void ScalarGetters()
        {
            Assert.Equal(-5L, Prim(TInt1, Le(-5, 1)).GetLong());
            Assert.Equal(-300L, Prim(TInt2, Le(-300, 2)).GetLong());
            Assert.Equal(100000L, Prim(TInt4, Le(100000, 4)).GetLong());
            Assert.Equal(9876543210L, Prim(TInt8, Le(9876543210L, 8)).GetLong());
            Assert.Equal(2.5, Prim(TDouble, F64(2.5)).GetDouble());
            Assert.True(Prim(TTrue).GetBoolean());
            Assert.False(Prim(TFalse).GetBoolean());
            var bin = Prim(TBinary, Combine(Le(4, 4), new byte[] { 1, 2, 3, 4 })).GetBinary();
            Assert.Equal(new byte[] { 1, 2, 3, 4 }, bin);
        }

        [Fact]
        public void NarrowedIntGetters()
        {
            // GetByte reads INT8 only.
            Assert.Equal((sbyte)-5, Prim(TInt1, Le(-5, 1)).GetByte());
            Assert.Throws<VariantException>(() => Prim(TInt2, Le(-300, 2)).GetByte());
            // GetShort widens INT8 -> INT16.
            Assert.Equal((short)-5, Prim(TInt1, Le(-5, 1)).GetShort());
            Assert.Equal((short)-300, Prim(TInt2, Le(-300, 2)).GetShort());
            Assert.Throws<VariantException>(() => Prim(TInt4, Le(100000, 4)).GetShort());
            // GetInt widens INT8/INT16 -> INT32.
            Assert.Equal(-5, Prim(TInt1, Le(-5, 1)).GetInt());
            Assert.Equal(-300, Prim(TInt2, Le(-300, 2)).GetInt());
            Assert.Equal(100000, Prim(TInt4, Le(100000, 4)).GetInt());
            Assert.Throws<VariantException>(() => Prim(TInt8, Le(9876543210L, 8)).GetInt());
        }

        [Fact]
        public void FloatAndDoubleAreExact()
        {
            // GetFloat reads FLOAT only; GetDouble reads DOUBLE only (no widening).
            Assert.Equal(2.5f, Prim(TFloat, F32(2.5f)).GetFloat());
            Assert.Equal(2.5, Prim(TDouble, F64(2.5)).GetDouble());
            Assert.Throws<VariantException>(() => Prim(TFloat, F32(2.5f)).GetDouble());
            Assert.Throws<VariantException>(() => Prim(TDouble, F64(2.5)).GetFloat());
        }

        [Theory]
        [InlineData(2, 4)]
        [InlineData(2, 8)]
        [InlineData(2, 16)]
        public void GetDecimalParts_PreservesScale(int scale, int width)
        {
            Prim(TDecimal4 + (width == 8 ? 1 : width == 16 ? 2 : 0), Dec(scale, 1234, width))
                .GetDecimalParts(out BigInteger unscaled, out int s);
            Assert.Equal(new BigInteger(1234), unscaled);
            Assert.Equal(scale, s);
        }

        [Theory]
        // Instant (TZ): seconds always present, 'Z', 0/3/6/9 fractional grouping.
        [InlineData(TTimestamp, 1577836800000000L, "\"2020-01-01T00:00:00Z\"")]
        [InlineData(TTimestamp, 1577836800123000L, "\"2020-01-01T00:00:00.123Z\"")]
        [InlineData(TTimestamp, 1577836800123456L, "\"2020-01-01T00:00:00.123456Z\"")]
        // NTZ: seconds always present, no zone.
        [InlineData(TTimestampNtz, 1577836800000000L, "\"2020-01-01T00:00:00\"")]
        [InlineData(TTimestampNtz, 1577836830000000L, "\"2020-01-01T00:00:30\"")]
        // Nanos: full precision.
        [InlineData(TTimestampNanos, 1577836800123456789L, "\"2020-01-01T00:00:00.123456789Z\"")]
        // Time: seconds always present.
        [InlineData(TTime, 45296123456L, "\"12:34:56.123456\"")]
        [InlineData(TTime, 45240000000L, "\"12:34:00\"")]
        // Date.
        [InlineData(TDate, 18262L, "\"2020-01-01\"")]
        public void ToJson_TemporalContract(int code, long raw, string expected)
        {
            int width = code == TDate ? 4 : 8;
            Assert.Equal(expected, Prim(code, Le(raw, width)).ToJson());
        }

        [Fact]
        public void ToJson_DecimalIsPlainAndScalePreserved()
        {
            Assert.Equal("0.0000001", Prim(TDecimal4, Dec(7, 1, 4)).ToJson());
            Assert.Equal("1.50", Prim(TDecimal4, Dec(2, 150, 4)).ToJson());
            Assert.Equal("12.34", Prim(TDecimal4, Dec(2, 1234, 4)).ToJson());
        }

        [Fact]
        public void ToJson_UuidAndBinary()
        {
            var uuidBytes = new byte[16];
            for (int i = 0; i < 16; i++) uuidBytes[i] = (byte)(0x11 * (i % 16));
            // Deterministic uuid bytes 00112233-4455-6677-8899-aabbccddeeff
            byte[] canonical = {
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
                0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff };
            Assert.Equal("\"00112233-4455-6677-8899-aabbccddeeff\"", Prim(TUuid, canonical).ToJson());
            Assert.Equal("\"" + Convert.ToBase64String(new byte[] { 0, 1, 2, 3 }) + "\"",
                Prim(TBinary, Combine(Le(4, 4), new byte[] { 0, 1, 2, 3 })).ToJson());
        }

        [Fact]
        public void ToJson_RoundTripStructure()
        {
            const string src = "{\"a\":1,\"b\":[true,null,\"x\"],\"c\":{\"d\":2}}";
            Assert.Equal(src, Variant.ParseJson(src).ToJson()); // sorted keys, compact
        }

        [Fact]
        public void Builder_NestedDoc_MatchesParseJson()
        {
            // A document using only JSON-representable types, so ParseJson produces identical bytes.
            // amount=150 does not fit INT8, so the JSON path selects INT16 - AppendShort matches it.
            const string json = "{\"id\":42,\"amount\":150," +
                "\"big\":123456789012345678901234567890,\"tags\":[\"x\",\"y\"]," +
                "\"nested\":{\"flag\":true,\"pi\":3.5},\"note\":null}";

            BigInteger big = BigInteger.Parse("123456789012345678901234567890");
            byte[] bigBe = big.ToByteArray();  // little-endian two's-complement
            Array.Reverse(bigBe);              // -> big-endian two's-complement

            var b = new VariantBuilder();
            b.StartObject();
            b.AppendKey("id"); b.AppendByte(42);
            b.AppendKey("amount"); b.AppendShort(150);
            b.AppendKey("big"); b.AppendDecimal(bigBe, 0);
            b.AppendKey("tags");
            b.StartArray();
            b.AppendString("x");
            b.AppendString("y");
            b.EndArray();
            b.AppendKey("nested");
            b.StartObject();
            b.AppendKey("flag"); b.AppendBoolean(true);
            b.AppendKey("pi"); b.AppendDouble(3.5);
            b.EndObject();
            b.AppendKey("note"); b.AppendNull();
            b.EndObject();
            Variant built = b.Build();

            Variant parsed = Variant.ParseJson(json);
            Assert.Equal(parsed.ToJson(), built.ToJson());
            Assert.Equal(parsed.ValueBytes, built.ValueBytes);       // byte-identical value
            Assert.Equal(parsed.MetadataBytes, built.MetadataBytes); // byte-identical metadata
        }

        [Fact]
        public void Builder_RootScalar_MatchesParseJson()
        {
            var b = new VariantBuilder();
            b.AppendString("hello");
            Variant built = b.Build();

            Variant parsed = Variant.ParseJson("\"hello\"");
            Assert.Equal("\"hello\"", built.ToJson());
            Assert.Equal(VariantType.String, built.GetVariantType());
            Assert.Equal(parsed.ValueBytes, built.ValueBytes);
            Assert.Equal(parsed.MetadataBytes, built.MetadataBytes);
        }

        [Fact]
        public void Builder_TypedScalars_RoundTripThroughReader()
        {
            var b = new VariantBuilder();
            b.StartArray();
            b.AppendLong(9876543210L);
            b.AppendFloat(2.5f);
            b.AppendUuid(new byte[] {
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
                0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff });
            b.AppendDate(18262);
            b.EndArray();
            Variant v = b.Build();

            Assert.Equal(VariantType.Array, v.GetVariantType());
            Assert.Equal(9876543210L, v.GetElementAtIndex(0).GetLong());
            Assert.Equal(2.5f, v.GetElementAtIndex(1).GetFloat());
            Assert.Equal("00112233-4455-6677-8899-aabbccddeeff", v.GetElementAtIndex(2).GetUuid());
            Assert.Equal(VariantType.Date, v.GetElementAtIndex(3).GetVariantType());
        }

        [Fact]
        public void LargeDataRegionUses4ByteOffsets()
        {
            // Regression: a container whose data region exceeds 0xFFFFFF (16 MiB)
            // requires 4-byte offsets. A single string element of length 16777216
            // pushes the array's data region past the 3-byte cap, exercising the
            // 4-byte size path. (~16 MiB alloc; takes a couple seconds.)
            const int len = 16777216; // 0x1000000
            var b = new VariantBuilder();
            b.StartArray();
            b.AppendString(new string('a', len));
            b.EndArray();
            Variant v = b.Build();

            Assert.Equal(VariantType.Array, v.GetVariantType());
            Assert.Equal(1, v.NumArrayElements());
            Assert.Equal(len, v.GetElementAtIndex(0).GetString().Length);
        }

        [Fact]
        public void Builder_Misuse_Throws()
        {
            // Build with an open container.
            Assert.Throws<VariantException>(() =>
            {
                var b = new VariantBuilder();
                b.StartObject();
                b.Build();
            });
            // AppendKey outside an object.
            Assert.Throws<VariantException>(() => new VariantBuilder().AppendKey("k"));
            // Value in an object without a preceding AppendKey.
            Assert.Throws<VariantException>(() =>
            {
                var b = new VariantBuilder();
                b.StartObject();
                b.AppendLong(1);
            });
            // Mismatched end.
            Assert.Throws<VariantException>(() =>
            {
                var b = new VariantBuilder();
                b.StartArray();
                b.EndObject();
            });
        }

        [Fact]
        public void Malformed_Throws()
        {
            Assert.Throws<VariantException>(() => new Variant(new byte[] { 0 }, new byte[] { 2, 0, 0 }));
            Assert.Throws<VariantException>(() => Prim(TTrue).GetString());
            Assert.Throws<VariantException>(() => Prim(TNull).GetLong());
        }

        private static byte[] Combine(byte[] a, byte[] b)
        {
            var r = new byte[a.Length + b.Length];
            Array.Copy(a, 0, r, 0, a.Length);
            Array.Copy(b, 0, r, a.Length, b.Length);
            return r;
        }
    }
}
