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
using Avro;
using Avro.Generic;
using Avro.Util;
using Newtonsoft.Json.Linq;

namespace Confluent.SchemaRegistry.Rules
{
    /// <summary>
    ///     Rebuilds a <see cref="GenericRecord" /> from the map a message-level <c>CEL</c>
    ///     transform returned.
    ///
    ///     A rule that returns a map is returning <b>the whole new record</b>: the transform has
    ///     replace semantics, not merge. So a field the rule does not name is dropped, a
    ///     <c>null</c> clears its field, and echoing a field that was absent materialises it.
    ///
    ///     Without this the executor's plain dictionary reached the Avro writer, which needs a
    ///     GenericRecord carrying the schema.
    ///
    ///     The schema comes from the record the rule ran against rather than being re-parsed:
    ///     the transform cannot change a message's type, so the original's schema is the one the
    ///     result conforms to.
    ///
    ///     <para>Lives beside <see cref="CelExecutor" /> rather than in the Avro serde, so that the
    ///     write-back runs where every other client runs it - inside the executor. It needs nothing
    ///     from that assembly: <c>Avro</c> and <c>Avro.Generic</c> reach here through Cel.NET, and
    ///     <see cref="Variant" /> is a core type.</para>
    /// </summary>
    internal static class AvroResultWriter
    {
        /// <summary>
        ///     Rebuilds <paramref name="original" />'s record type from <paramref name="result" />,
        ///     or returns <paramref name="result" /> unchanged when it is not a map.
        /// </summary>
        public static object Convert(object original, object result)
        {
            if (!(result is IDictionary values) || !(original is GenericRecord src))
            {
                return result;
            }

            return BuildRecord(src.Schema, values);
        }

        private static GenericRecord BuildRecord(RecordSchema schema, IDictionary values)
        {
            var record = new GenericRecord(schema);
            var named = new HashSet<string>();
            foreach (DictionaryEntry entry in values)
            {
                string name = entry.Key?.ToString();
                if (name == null || !schema.TryGetField(name, out Field field))
                {
                    // A key the schema does not declare has nowhere to go, which matches how
                    // the other clients drop an unknown field.
                    continue;
                }

                named.Add(field.Name);
                if (entry.Value == null)
                {
                    // An explicit null clears the field. Avro needs the field present, so it is
                    // written as null rather than left out - a record with a missing field
                    // cannot be encoded at all.
                    record.Add(name, null);
                    continue;
                }

                record.Add(name, ConvertValue(field.Schema, entry.Value));
            }

            // Replace semantics leave every unnamed field to be filled in from the schema, since
            // Avro has no absent field. This mirrors GenericRecordBuilder.build() on the JVM: the
            // declared default, or a failure naming the field.
            //
            // Before this the field was simply left out, and Apache.Avro's writer raised a bare
            // NullReferenceException from deep inside the encode - true but useless, and it fired
            // even when the schema declared a perfectly good default.
            foreach (Field field in schema.Fields)
            {
                if (named.Contains(field.Name))
                {
                    continue;
                }

                if (field.DefaultValue == null)
                {
                    throw new RuleException(
                        "CEL transform result does not set field '" + field.Name + "' of "
                        + schema.Fullname + ", which has no default value");
                }

                record.Add(field.Name, DefaultOf(field.Schema, field.DefaultValue, field.Name));
            }

            return record;
        }

        /// <summary>
        ///     Materialises a field's declared default, which Avro stores as the raw JSON it was
        ///     written as.
        ///
        ///     Mirrors <c>Avro.IO.Resolver.EncodeDefaultValue</c> case for case - that is Avro's own
        ///     reading of a default and the one its writer will accept - but yields an object rather
        ///     than encoding it, since the value is going into a <see cref="GenericRecord" />. It
        ///     cannot simply be called: <c>Resolver</c> is internal to the Avro assembly.
        /// </summary>
        private static object DefaultOf(Avro.Schema schema, JToken json, string fieldName)
        {
            switch (schema.Tag)
            {
                case Avro.Schema.Type.Null:
                    return null;
                case Avro.Schema.Type.Boolean:
                    return (bool)json;
                case Avro.Schema.Type.Int:
                    return (int)json;
                case Avro.Schema.Type.Long:
                    return (long)json;
                case Avro.Schema.Type.Float:
                    return (float)json;
                case Avro.Schema.Type.Double:
                    return (double)json;
                case Avro.Schema.Type.String:
                    return (string)json;
                case Avro.Schema.Type.Bytes:
                    return DefaultBytes(json);
                case Avro.Schema.Type.Fixed:
                    return new GenericFixed((FixedSchema)schema, DefaultBytes(json));
                case Avro.Schema.Type.Enumeration:
                    return new GenericEnum((EnumSchema)schema, (string)json);
                case Avro.Schema.Type.Array:
                {
                    // An array, not a List, for the same reason as ConvertValue's array arm:
                    // Apache.Avro's writer rejects anything that is not a System.Array.
                    var items = new List<object>();
                    foreach (JToken item in (JArray)json)
                    {
                        items.Add(DefaultOf(((ArraySchema)schema).ItemSchema, item, fieldName));
                    }

                    return items.ToArray();
                }
                case Avro.Schema.Type.Map:
                {
                    var entries = new Dictionary<string, object>();
                    foreach (var pair in (JObject)json)
                    {
                        entries[pair.Key] =
                            DefaultOf(((MapSchema)schema).ValueSchema, pair.Value, fieldName);
                    }

                    return entries;
                }
                case Avro.Schema.Type.Record:
                case Avro.Schema.Type.Error:
                {
                    var nested = (RecordSchema)schema;
                    var built = new GenericRecord(nested);
                    var obj = (JObject)json;
                    foreach (Field f in nested.Fields)
                    {
                        JToken val = obj[f.Name] ?? f.DefaultValue;
                        if (val == null)
                        {
                            throw new RuleException(
                                "the default for field '" + fieldName + "' does not set '"
                                + f.Name + "', which has no default of its own");
                        }

                        built.Add(f.Name, DefaultOf(f.Schema, val, fieldName));
                    }

                    return built;
                }
                case Avro.Schema.Type.Union:
                    // A union's default is written against its first branch, per the Avro spec.
                    return DefaultOf(((UnionSchema)schema).Schemas[0], json, fieldName);
                case Avro.Schema.Type.Logical:
                {
                    // The default is written against the underlying type, so it is read that way
                    // and then lifted - the writer expects the logical representation (an
                    // AvroDecimal, a DateTime), not the bytes or long underneath it.
                    var logical = (LogicalSchema)schema;
                    object baseValue = DefaultOf(logical.BaseSchema, json, fieldName);
                    return logical.LogicalType.ConvertToLogicalValue(baseValue, logical);
                }
                default:
                    throw new RuleException(
                        "cannot read the default for field '" + fieldName + "': unsupported type "
                        + schema.Tag);
            }
        }

        /// <summary>
        ///     Avro writes a bytes or fixed default as a JSON string of code points 0-255, one per
        ///     byte, which is ISO-8859-1 - the same encoding Avro's own Resolver uses to read it.
        /// </summary>
        private static byte[] DefaultBytes(JToken json)
        {
            return System.Text.Encoding.GetEncoding("iso-8859-1").GetBytes((string)json);
        }

        /// <summary>
        ///     Converts one CEL value into the shape Avro's writer expects for that field.
        ///     A decimal arrives as a <see cref="BigDecimal" /> and a timestamp as a NodaTime
        ///     value; both need their Avro representation. A <see cref="Variant" /> already is
        ///     its Avro representation - <c>VariantLogicalType</c> encodes it directly.
        /// </summary>
        private static object ConvertValue(Avro.Schema schema, object value)
        {
            if (value == null || value is Google.Protobuf.WellKnownTypes.NullValue)
            {
                // CEL null. Avro writes a union's null branch from a CLR null; Cel.NET's own
                // representation is NullValue, a protobuf enum, which Avro's writer rejects with
                // "Cannot find a match for ...NullValue in [\"null\", ...]". That hit the two
                // forms a rule author is most likely to write: an identity pass-through over a
                // nullable field, and the `has(x) ? x : null` guard that is the only way to
                // preserve absence.
                return null;
            }

            if (value is BigDecimal bigDecimal)
            {
                // Written out rather than calling AvroDecimalExtensions.ToAvroDecimal, which is
                // public API of the Avro serde assembly - not referenced from here, and not worth
                // a reference for one constructor call.
                return new AvroDecimal(bigDecimal.Unscaled, bigDecimal.Scale);
            }

            if (value is Variant)
            {
                return value;
            }

            // The CEL runtime hands timestamps back as NodaTime values. Unlike the protobuf
            // writer there is no precision to preserve here: Avro's temporal logical types are
            // carried as DateTime, whose tick is 100 nanoseconds, so the instant is rounded to
            // it either way. Naming the type is simply checked at compile time.
            if (value is NodaTime.ZonedDateTime zoned)
            {
                return zoned.ToDateTimeUtc();
            }

            if (value is NodaTime.Instant instant)
            {
                return instant.ToDateTimeUtc();
            }

            if (value is DateTimeOffset offset)
            {
                return offset.UtcDateTime;
            }

            if (value is IDictionary nested && Unwrap(schema) is RecordSchema recordSchema)
            {
                return BuildRecord(recordSchema, nested);
            }

            if (value is IDictionary entries && Unwrap(schema) is MapSchema mapSchema)
            {
                // Avro's writer wants IDictionary<string, object> specifically, and there was no
                // map arm here at all - a CEL map arrived as Dictionary<object, object> and went
                // through untouched, so every message transform over a schema with a map field
                // failed with "IDictionary<string, object> required to write against map schema
                // but found Dictionary`2[System.Object, ...]". Converting the values also gives
                // a decimal or timestamp inside a map the same treatment as one at top level.
                var converted = new Dictionary<string, object>();
                foreach (DictionaryEntry entry in entries)
                {
                    converted[System.Convert.ToString(entry.Key)] = entry.Value == null
                        ? null
                        : ConvertValue(mapSchema.ValueSchema, entry.Value);
                }

                return converted;
            }

            if (value is IList items && Unwrap(schema) is ArraySchema arraySchema)
            {
                // An array, not a List: Apache.Avro's writer type-checks the value against the
                // schema and rejects anything that is not a System.Array with "Array required to
                // write against array schema but found System.Collections.Generic.List`1". That
                // made *every* rule writing back over a schema with an array field fail,
                // whatever it computed - a field transform on the array (C5) and a message
                // transform naming it (C6, C7) alike. It is also the shape the deserializer
                // hands back and the shape a caller builds a GenericRecord with, so returning
                // an array keeps the rule's output indistinguishable from an untouched value.
                var converted = new object[items.Count];
                for (int i = 0; i < items.Count; i++)
                {
                    object item = items[i];
                    converted[i] = item == null ? null : ConvertValue(arraySchema.ItemSchema, item);
                }

                return converted;
            }

            return value;
        }

        /// <summary>
        ///     A nullable field is a union of null and the branch that carries the value; the
        ///     branch is what a converted value has to match.
        /// </summary>
        private static Avro.Schema Unwrap(Avro.Schema schema)
        {
            if (schema is UnionSchema union)
            {
                foreach (Avro.Schema branch in union.Schemas)
                {
                    if (branch.Tag != Avro.Schema.Type.Null)
                    {
                        return branch;
                    }
                }
            }

            return schema;
        }
    }
}
