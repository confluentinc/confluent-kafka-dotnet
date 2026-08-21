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
    ///     Builds Variant (value, metadata) bytes from parsed JSON. Number handling follows Java
    ///     VariantUtils.fromJsonNode: a fractional JSON number becomes a DOUBLE; an integer becomes
    ///     the smallest int1/2/4/8 that fits, or a scale-0 decimal when wider than 64 bits.
    ///     Newtonsoft distinguishes an integer token from a fractional one, so `1` and `1.0` are
    ///     classified as Java does.
    /// </summary>
    internal sealed class VariantBuilder
    {
        private readonly List<byte> value = new List<byte>();
        private readonly Dictionary<string, int> dictionary = new Dictionary<string, int>();
        private readonly List<byte[]> dictionaryKeys = new List<byte[]>();

        public static void Build(string json, out byte[] value, out byte[] metadata)
        {
            var builder = new VariantBuilder();
            builder.ProcessJson(JToken.Parse(json));
            builder.Finish(out value, out metadata);
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

        private void ProcessJson(JToken token)
        {
            switch (token.Type)
            {
                case JTokenType.Object:
                {
                    var fields = new List<FieldEntry>();
                    int start = value.Count;
                    foreach (JProperty prop in ((JObject)token).Properties())
                    {
                        int id = AddKey(prop.Name);
                        fields.Add(new FieldEntry(prop.Name, id, value.Count - start));
                        ProcessJson(prop.Value);
                    }
                    FinishWritingObject(start, fields);
                    break;
                }
                case JTokenType.Array:
                {
                    var offsets = new List<int>();
                    int start = value.Count;
                    foreach (JToken el in (JArray)token)
                    {
                        offsets.Add(value.Count - start);
                        ProcessJson(el);
                    }
                    FinishWritingArray(start, offsets);
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
                        if (bi >= long.MinValue && bi <= long.MaxValue) AppendInt((long)bi);
                        else AppendDecimal(bi, 0);
                    }
                    else
                    {
                        AppendInt(Convert.ToInt64(raw, CultureInfo.InvariantCulture));
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

        private int AddKey(string key)
        {
            if (dictionary.TryGetValue(key, out int existing)) return existing;
            int id = dictionaryKeys.Count;
            dictionary[key] = id;
            dictionaryKeys.Add(Encoding.UTF8.GetBytes(key));
            return id;
        }

        private static byte PrimitiveHeader(int typeCode) => (byte)((typeCode << 2) | Variant.Primitive);

        private void AppendBoolean(bool b) =>
            value.Add(PrimitiveHeader(b ? Variant.TTrue : Variant.TFalse));

        private void AppendNull() => value.Add(PrimitiveHeader(Variant.TNull));

        private void AppendString(string s)
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

        private void AppendInt(long i)
        {
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

        private void AppendDecimal(BigInteger unscaled, int scale)
        {
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

        private void AppendDouble(double d)
        {
            value.Add(PrimitiveHeader(Variant.TDouble));
            byte[] b = BitConverter.GetBytes(d);
            if (!BitConverter.IsLittleEndian) Array.Reverse(b);
            value.AddRange(b);
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
            fields.Sort((a, b) => string.CompareOrdinal(a.Key, b.Key));
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

        private static int IntegerSize(int v)
        {
            if (v <= 0xFF) return 1;
            if (v <= 0xFFFF) return 2;
            return 3;
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
    }
}
