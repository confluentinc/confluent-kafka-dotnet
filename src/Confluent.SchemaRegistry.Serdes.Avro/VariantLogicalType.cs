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

        /// <summary>
        ///     Runs the registration once. <see cref="LazyThreadSafetyMode.ExecutionAndPublication" />
        ///     is the point: a competing caller blocks until registration has completed rather
        ///     than proceeding as soon as the attempt has started. An <c>Interlocked</c> flag set
        ///     up front let a second thread parse a variant schema while the first was still
        ///     inside <c>Register</c>, so the logical type was not yet there to be found. This is
        ///     what the reference gets from a JVM static initializer, which runs exactly once and
        ///     blocks other threads until it returns (AvroSchemaUtils' static block).
        /// </summary>
        private static readonly Lazy<bool> registration = new Lazy<bool>(
            () =>
            {
                // Apache.Avro's Register "registers or replaces", and the registry is keyed by
                // logical type name. The reference registers only if absent, so that a library
                // which claimed "variant" first keeps it - Apache Iceberg registers the same
                // name. Apache.Avro exposes no way to ask whether a name is taken, so that
                // guard cannot be reproduced here.
                LogicalTypeFactory.Instance.Register(new VariantLogicalType());
                return true;
            },
            LazyThreadSafetyMode.ExecutionAndPublication);

        /// <summary>
        ///     Registers the variant logical type with the process-wide
        ///     <see cref="LogicalTypeFactory" /> exactly once. Called from the Avro serializer and
        ///     deserializer constructors so a <c>confluent.type.Variant</c> record surfaces as a
        ///     <see cref="Variant" /> without any explicit setup by the caller.
        /// </summary>
        public static void EnsureRegistered()
        {
            _ = registration.Value;
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
            // No metadata bytes means an absent variant, which the reference reports as null
            // (VariantUtils.toVariant: "the Variant, or null when there are no metadata bytes").
            // Constructing one here would index metadata[0] and throw before the CEL layer could
            // ever see the absence.
            return metadata == null || metadata.Length == 0 ? null : new Variant(value, metadata);
        }

        /// <summary>
        ///     Returns the CLR type a variant field surfaces as, <see cref="Variant" />.
        /// </summary>
        public override Type GetCSharpType(bool nullable) => typeof(Variant);

        /// <summary>
        ///     Builds the base Avro record for a <see cref="Variant" /> against a plain
        ///     <see cref="RecordSchema" />.
        ///
        ///     <para>
        ///         <see cref="ConvertToBaseValue" /> needs a <see cref="LogicalSchema" />, which is
        ///         precisely what a by-name reference site does not have. This is the
        ///         same two lines against the record schema directly, so the reference site and the
        ///         definition site encode identically rather than through two conversions that
        ///         could drift.
        ///     </para>
        /// </summary>
        internal static GenericRecord ToBaseRecord(Variant variant, RecordSchema schema)
        {
            var record = new GenericRecord(schema);
            record.Add("metadata", variant.MetadataBytes);
            record.Add("value", variant.ValueBytes);
            return record;
        }

        /// <summary>
        ///     Returns whether <paramref name="logicalValue" /> is a <see cref="Variant" />.
        /// </summary>
        public override bool IsInstanceOfLogicalType(object logicalValue) => logicalValue is Variant;

        /// <summary>
        ///     Validates that the base schema is a record with exactly the two <c>bytes</c> fields
        ///     <c>metadata</c> and <c>value</c>.
        /// </summary>
        /// <remarks>
        ///     The field names and types are checked, not just the field count, because
        ///     <see cref="ConvertToBaseValue" /> and <see cref="ConvertToLogicalValue" /> index the
        ///     record by those names and cast both to <c>byte[]</c>. Without the full check a
        ///     two-field record of any shape passed validation and then failed later with a
        ///     KeyNotFoundException or an InvalidCastException. Matches the Java reference, whose
        ///     VariantLogicalType.isVariantSchema applies exactly these conditions.
        /// </remarks>
        public override void ValidateSchema(LogicalSchema schema)
        {
            if (!(schema.BaseSchema is RecordSchema record)
                || record.Fields.Count != 2
                || !record.TryGetField("metadata", out Field metadataField)
                || metadataField.Schema.Tag != Avro.Schema.Type.Bytes
                || !record.TryGetField("value", out Field valueField)
                || valueField.Schema.Tag != Avro.Schema.Type.Bytes)
            {
                throw new AvroTypeException(
                    "variant logical type requires a record with 'metadata' and 'value' bytes fields");
            }
        }
    }
}
