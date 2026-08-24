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
using Google.Protobuf;
using Google.Protobuf.Reflection;
using SrVariant = Confluent.SchemaRegistry.Variant;

namespace Confluent.SchemaRegistry.Rules
{
    /// <summary>
    ///     Conversion helpers backing <c>variant(...)</c> and the <c>variants.*</c> accessor
    ///     functions — the CEL counterpart of Java's variant glue. The CEL surface treats
    ///     Variant as the canonical type <see cref="CelTypeLabels.VariantName" />; this client
    ///     backs it with <see cref="SrVariant" />. Mirrors <see cref="DecimalUtils" />.
    /// </summary>
    internal static class VariantUtils
    {
        // VariantType -> the coarse label variants.type returns, matching the Java
        // variantTypeName / the Python and JS clients: integer widths collapse to "int",
        // float/double to "double", decimal widths to "decimal", and all four timestamp
        // variants to "timestamp".
        private static readonly IDictionary<VariantType, string> TypeLabels =
            new Dictionary<VariantType, string>
            {
                { VariantType.Object, "object" },
                { VariantType.Array, "array" },
                { VariantType.Null, "null" },
                { VariantType.Boolean, "boolean" },
                { VariantType.Byte, "int" },
                { VariantType.Short, "int" },
                { VariantType.Int, "int" },
                { VariantType.Long, "int" },
                { VariantType.Float, "double" },
                { VariantType.Double, "double" },
                { VariantType.Decimal4, "decimal" },
                { VariantType.Decimal8, "decimal" },
                { VariantType.Decimal16, "decimal" },
                { VariantType.Date, "date" },
                { VariantType.Time, "time" },
                { VariantType.TimestampTz, "timestamp" },
                { VariantType.TimestampNtz, "timestamp" },
                { VariantType.TimestampNanosTz, "timestamp" },
                { VariantType.TimestampNanosNtz, "timestamp" },
                { VariantType.String, "string" },
                { VariantType.Binary, "bytes" },
                { VariantType.Uuid, "uuid" },
            };

        /// <summary>The coarse type label <c>variants.type</c> returns for a variant.</summary>
        public static string TypeLabel(VariantType type)
        {
            if (TypeLabels.TryGetValue(type, out string label))
            {
                return label;
            }

            throw new ArgumentException("Unsupported variant type: " + type);
        }

        private static byte[] CoerceBytes(object v)
        {
            switch (v)
            {
                case byte[] b:
                    return b;
                case ByteString bs:
                    return bs.ToByteArray();
                default:
                    throw new ArgumentException(
                        "variant: expected bytes, got " + (v == null ? "null" : v.GetType().Name));
            }
        }

        /// <summary>
        ///     Runtime dispatch backing <c>variant(dyn)</c>: accept the shapes proto/Avro
        ///     decoders produce — a <see cref="SrVariant" /> (Avro variant logical type),
        ///     a <c>confluent.type.Variant</c> proto message, or a map with <c>metadata</c>
        ///     and <c>value</c> byte entries. Rejects strings (use <c>variants.parseJson</c>).
        /// </summary>
        public static SrVariant ToVariant(object o)
        {
            switch (o)
            {
                case null:
                    throw new ArgumentException("variant: cannot convert null to Variant");
                case SrVariant v:
                    return v;
                case IMessage msg when msg.Descriptor.FullName == CelTypeLabels.VariantName:
                    return FromMessage(msg);
                case string _:
                    throw new ArgumentException(
                        "variant: cannot convert string to Variant; use variants.parseJson(s) "
                        + "for strict JSON parsing or variants.tryParseJson(s) for soft mode");
                case IDictionary dict:
                    return FromMap(dict);
                default:
                    throw new ArgumentException(
                        "variant: cannot convert " + o.GetType().FullName + " to Variant");
            }
        }

        private static SrVariant FromMessage(IMessage message)
        {
            MessageDescriptor descriptor = message.Descriptor;
            FieldDescriptor metadataField = descriptor.FindFieldByName("metadata");
            FieldDescriptor valueField = descriptor.FindFieldByName("value");
            if (metadataField == null || valueField == null)
            {
                throw new ArgumentException(
                    "confluent.type.Variant message missing required field: "
                    + (metadataField == null ? "'metadata'" : "'value'"));
            }

            var metadata = (ByteString)metadataField.Accessor.GetValue(message);
            var value = (ByteString)valueField.Accessor.GetValue(message);
            return new SrVariant(
                value == null ? Array.Empty<byte>() : value.ToByteArray(),
                metadata == null ? Array.Empty<byte>() : metadata.ToByteArray());
        }

        private static SrVariant FromMap(IDictionary dict)
        {
            object metadata = dict["metadata"];
            object value = dict["value"];
            if (metadata != null && value != null)
            {
                return new SrVariant(CoerceBytes(value), CoerceBytes(metadata));
            }

            if (metadata != null || value != null)
            {
                throw new ArgumentException(
                    "variant: cannot convert map to Variant: missing '"
                    + (value == null ? "value" : "metadata") + "' entry");
            }

            throw new ArgumentException("variant: cannot convert map to Variant");
        }
    }
}
