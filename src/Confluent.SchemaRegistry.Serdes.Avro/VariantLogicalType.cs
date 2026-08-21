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
using System.Threading;
using Avro;
using Avro.Generic;
using Avro.Util;

namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     The Avro <c>variant</c> logical type: a record of two <c>bytes</c> fields
    ///     (<c>metadata</c>, <c>value</c>) carrying a Spark/Parquet Variant. A field with this
    ///     logical type decodes to / encodes from a <see cref="Variant" />, so serde consumers and
    ///     CEL rules see a first-class Variant. The Avro serializer and deserializer register it
    ///     automatically via <see cref="EnsureRegistered" />. This is the .NET counterpart of
    ///     Java's <c>io.confluent.avro.type.VariantConversion</c>.
    /// </summary>
    public class VariantLogicalType : LogicalType
    {
        /// <summary>
        ///     The Avro logical type name, <c>"variant"</c>.
        /// </summary>
        public const string LogicalTypeName = "variant";

        private static int registered;

        /// <summary>
        ///     Registers the variant logical type with the process-wide
        ///     <see cref="LogicalTypeFactory" /> exactly once. Called from the Avro serializer and
        ///     deserializer constructors so a <c>confluent.type.Variant</c> record surfaces as a
        ///     <see cref="Variant" /> without any explicit setup by the caller.
        /// </summary>
        public static void EnsureRegistered()
        {
            if (Interlocked.Exchange(ref registered, 1) == 0)
            {
                LogicalTypeFactory.Instance.Register(new VariantLogicalType());
            }
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="VariantLogicalType" /> class.
        /// </summary>
        public VariantLogicalType() : base(LogicalTypeName)
        {
        }

        /// <summary>
        ///     Converts a <see cref="Variant" /> to its base Avro record (two <c>bytes</c> fields).
        /// </summary>
        public override object ConvertToBaseValue(object logicalValue, LogicalSchema schema)
        {
            var variant = (Variant)logicalValue;
            var record = new GenericRecord((RecordSchema)schema.BaseSchema);
            record.Add("metadata", variant.MetadataBytes);
            record.Add("value", variant.ValueBytes);
            return record;
        }

        /// <summary>
        ///     Converts a base Avro record (two <c>bytes</c> fields) to a <see cref="Variant" />.
        /// </summary>
        public override object ConvertToLogicalValue(object baseValue, LogicalSchema schema)
        {
            var record = (GenericRecord)baseValue;
            var metadata = (byte[])record["metadata"];
            var value = (byte[])record["value"];
            return new Variant(value, metadata);
        }

        /// <summary>
        ///     Returns the CLR type a variant field surfaces as, <see cref="Variant" />.
        /// </summary>
        public override Type GetCSharpType(bool nullable) => typeof(Variant);

        /// <summary>
        ///     Returns whether <paramref name="logicalValue" /> is a <see cref="Variant" />.
        /// </summary>
        public override bool IsInstanceOfLogicalType(object logicalValue) => logicalValue is Variant;

        /// <summary>
        ///     Validates that the base schema is a record with two <c>bytes</c> fields.
        /// </summary>
        public override void ValidateSchema(LogicalSchema schema)
        {
            if (!(schema.BaseSchema is RecordSchema record) || record.Fields.Count != 2)
            {
                throw new AvroTypeException(
                    "variant logical type requires a record with 'metadata' and 'value' bytes fields");
            }
        }
    }
}
