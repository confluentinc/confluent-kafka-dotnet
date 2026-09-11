// Copyright 2022 Confluent Inc.
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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Avro;
using Avro.Generic;
using Avro.Specific;
using Newtonsoft.Json;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     Avro utilities
    /// </summary>
    public static class AvroUtils
    {
        /// <summary>
        ///     Whether a schema has a <c>confluent.type.Variant</c> record that is *not* wrapped
        ///     in a logical type, i.e. a by-name reference site. Cached, because the answer is a
        ///     property of the schema and the walk below is skipped entirely when it is false -
        ///     which it is for every schema that does not use a variant twice.
        /// </summary>
        private const int MaxCachedSchemas = 1000;

        private static readonly ConcurrentDictionary<Avro.Schema, bool> hasBareVariant =
            new ConcurrentDictionary<Avro.Schema, bool>();

        /// <summary>
        ///     Converts a <see cref="Variant" /> into its base record wherever the schema carries
        ///     a bare <c>confluent.type.Variant</c> record, so the generic writer accepts it.
        ///
        ///     <para>
        ///         Apache.Avro applies the <c>variant</c> logical
        ///         type only where the schema *defines* the record; a by-name reference resolves to
        ///         a plain <see cref="RecordSchema" />, and the writer then rejects the
        ///         <see cref="Variant" /> with "GenericRecord required to write against record
        ///         schema". <see cref="VariantSchemaRebinder" /> fixes this for every schema the
        ///         client parses itself, but on the generic write path the writer schema is
        ///         <c>data.Schema</c> - the caller's own parse - so the value is what has to give.
        ///     </para>
        ///     <para>
        ///         Re-parsing the caller's schema instead is not available: the writer asserts
        ///         <c>record.Schema.Equals(writerSchema)</c>, so a rebound schema would be refused
        ///         by the very records it was meant to help.
        ///     </para>
        /// </summary>
        internal static object BindVariantsForWriter(Avro.Schema schema, object value)
        {
            if (!HasBareVariantCached(schema))
            {
                return value;
            }

            return BindVariants(schema, value);
        }

        /// <summary>
        ///     Whether the schema reaches a bare variant, remembering the answer.
        ///
        ///     <para>
        ///         Bounded, because the key is a schema the caller created: a producer building
        ///         schemas dynamically would otherwise retain every schema graph it had ever
        ///         serialized, including the ones the answer was <c>false</c> for. The same
        ///         ceiling the reference puts on its parse cache.
        ///     </para>
        /// </summary>
        private static bool HasBareVariantCached(Avro.Schema schema)
        {
            if (hasBareVariant.TryGetValue(schema, out bool cached))
            {
                return cached;
            }

            bool answer = HasBareVariantRecord(schema, new HashSet<string>());
            if (hasBareVariant.Count >= MaxCachedSchemas)
            {
                hasBareVariant.Clear();
            }

            hasBareVariant[schema] = answer;
            return answer;
        }

        private static object BindVariants(Avro.Schema schema, object value)
        {
            if (value == null)
            {
                return value;
            }

            switch (schema.Tag)
            {
                case Avro.Schema.Type.Union:
                    // Resolve the branch this value takes. A variant can hide inside any
                    // container branch, not only a record one, so an optional array or map of
                    // variants has to be descended into as well.
                    foreach (Avro.Schema branch in ((UnionSchema)schema).Schemas)
                    {
                        if (branch.Tag != Avro.Schema.Type.Null && BranchAccepts(branch, value))
                        {
                            return BindVariants(branch, value);
                        }
                    }

                    return value;

                case Avro.Schema.Type.Array:
                {
                    if (!(value is IList list))
                    {
                        return value;
                    }

                    Avro.Schema items = ((ArraySchema)schema).ItemSchema;
                    var boundItems = new object[list.Count];
                    bool itemChanged = false;
                    for (int i = 0; i < list.Count; i++)
                    {
                        boundItems[i] = BindVariants(items, list[i]);
                        itemChanged |= !ReferenceEquals(boundItems[i], list[i]);
                    }

                    // Only hand back a new collection when something in it actually changed, so
                    // the record arm below can tell an untouched field from a rebound one.
                    return itemChanged ? boundItems : value;
                }

                case Avro.Schema.Type.Map:
                {
                    if (!(value is IDictionary map))
                    {
                        return value;
                    }

                    Avro.Schema values = ((MapSchema)schema).ValueSchema;
                    var boundValues = new Dictionary<string, object>(map.Count);
                    bool valueChanged = false;
                    foreach (DictionaryEntry entry in map)
                    {
                        object bound = BindVariants(values, entry.Value);
                        valueChanged |= !ReferenceEquals(bound, entry.Value);
                        boundValues[System.Convert.ToString(entry.Key)] = bound;
                    }

                    return valueChanged ? (object)boundValues : value;
                }

                case Avro.Schema.Type.Record:
                {
                    var recordSchema = (RecordSchema)schema;
                    if (value is Variant variant && IsVariantRecord(recordSchema))
                    {
                        return VariantLogicalType.ToBaseRecord(variant, recordSchema);
                    }

                    if (!(value is GenericRecord record))
                    {
                        return value;
                    }

                    // Copy on the first change, and only then: the record belongs to the caller
                    // and the serializer hands it here immediately before encoding, so writing
                    // into it would both alter the caller's own object and race with a
                    // concurrent serialization of the same record. A field that needs no
                    // rebinding is shared with the original rather than copied.
                    GenericRecord copy = null;
                    foreach (Field field in recordSchema.Fields)
                    {
                        if (!record.TryGetValue(field.Name, out object fieldValue))
                        {
                            continue;
                        }

                        object bound = BindVariants(field.Schema, fieldValue);
                        if (ReferenceEquals(bound, fieldValue))
                        {
                            continue;
                        }

                        if (copy == null)
                        {
                            copy = ShallowCopy(record, recordSchema);
                        }

                        copy.Add(field.Pos, bound);
                    }

                    return copy ?? (object)record;
                }

                default:
                    return value;
            }
        }

        /// <summary>
        ///     A copy carrying the same schema instance and field values, so the generic writer's
        ///     <c>record.Schema.Equals(writerSchema)</c> check still holds for the copy.
        /// </summary>
        private static GenericRecord ShallowCopy(GenericRecord source, RecordSchema schema)
        {
            var copy = new GenericRecord(source.Schema);
            foreach (Field field in schema.Fields)
            {
                if (source.TryGetValue(field.Name, out object existing))
                {
                    copy.Add(field.Pos, existing);
                }
            }

            return copy;
        }

        /// <summary>
        ///     Whether a union branch is the one this value takes. A union may hold several
        ///     non-null branches, so the branch cannot simply be the first that is not null:
        ///     with <c>["null", array, map]</c> a map value tested against the array branch would
        ///     never be descended into.
        /// </summary>
        private static bool BranchAccepts(Avro.Schema branch, object value)
        {
            switch (branch.Tag)
            {
                case Avro.Schema.Type.Record:
                    return value is Variant
                        ? IsVariantRecord(branch)
                        : value is GenericRecord rec
                          && rec.Schema.Fullname == ((RecordSchema)branch).Fullname;
                case Avro.Schema.Type.Map:
                    return value is IDictionary;
                case Avro.Schema.Type.Array:
                    return value is IList;
                default:
                    return false;
            }
        }

        /// <summary>
        ///     A <c>confluent.type.Variant</c> record with no logical type on it. A definition site
        ///     is a <c>LogicalSchema</c> and never reaches here, which is what keeps this to the
        ///     reference sites.
        /// </summary>
        private static bool IsVariantRecord(Avro.Schema schema) =>
            schema.Tag == Avro.Schema.Type.Record
            && ((RecordSchema)schema).Fullname == VariantSchemaRebinder.VariantFullName;

        /// <summary>
        ///     Whether the schema reaches a bare variant record. <paramref name="seen" /> guards
        ///     the recursive-schema case, which would otherwise not terminate.
        /// </summary>
        private static bool HasBareVariantRecord(Avro.Schema schema, HashSet<string> seen)
        {
            switch (schema.Tag)
            {
                case Avro.Schema.Type.Union:
                    return ((UnionSchema)schema).Schemas.Any(b => HasBareVariantRecord(b, seen));
                case Avro.Schema.Type.Array:
                    return HasBareVariantRecord(((ArraySchema)schema).ItemSchema, seen);
                case Avro.Schema.Type.Map:
                    return HasBareVariantRecord(((MapSchema)schema).ValueSchema, seen);
                case Avro.Schema.Type.Record:
                    var recordSchema = (RecordSchema)schema;
                    if (IsVariantRecord(recordSchema))
                    {
                        return true;
                    }

                    if (!seen.Add(recordSchema.Fullname))
                    {
                        return false;
                    }

                    return recordSchema.Fields.Any(f => HasBareVariantRecord(f.Schema, seen));
                default:
                    return false;
            }
        }

        /// <summary>
        ///     Resolves named schemas referenced by the provided schema recursively.
        /// </summary>
        /// <param name="schema">
        ///     Schema to resolve named schemas for.
        /// </param>
        /// <param name="schemaRegistryClient">
        ///     SchemaRegistryClient to use for retrieval.
        /// </param>
        /// <returns>
        ///     A SchemaNames object containing the resolved named schemas.
        /// </returns>
        public static async Task<SchemaNames> ResolveNamedSchema(Schema schema, ISchemaRegistryClient schemaRegistryClient)
        {
            var namedSchemas = new SchemaNames();

            if (schema.References != null)
            {
                foreach (var reference in schema.References)
                {
                    var referencedSchema = await schemaRegistryClient
                        .GetRegisteredSchemaAsync(reference.Subject, reference.Version, false)
                        .ConfigureAwait(continueOnCapturedContext: false);

                    var refNamedSchemas = await ResolveNamedSchema(referencedSchema, schemaRegistryClient)
                        .ConfigureAwait(continueOnCapturedContext: false);

                    // Not a cast: a referenced schema whose top level declares a logical type
                    // parses to a LogicalSchema, which is not a NamedSchema, and casting threw
                    // before the root schema was ever parsed. Registering the wrapper's base
                    // schema under its name is what Avro Java gets for free - there a logical
                    // type is an attribute of the Schema rather than a wrapper around it, so one
                    // object is both named and logical.
                    // Rebound like the root schema is: a referenced schema may define the
                    // variant once and reference it by name elsewhere within itself, and that
                    // nested reference would otherwise stay a bare record no matter how the
                    // root is parsed - so a field behind it would not surface as a Variant.
                    Avro.Schema parsed = Avro.Schema.Parse(
                        VariantSchemaRebinder.Rebind(referencedSchema.SchemaString),
                        refNamedSchemas);
                    var parsedSchema = parsed as NamedSchema
                        ?? (parsed as LogicalSchema)?.BaseSchema as NamedSchema;

                    // Add all schemas from refNamedSchemas to namedSchemas
                    foreach (var kvp in refNamedSchemas.Names)
                    {
                        if (!namedSchemas.Contains(kvp.Key))
                        {
                            namedSchemas.Add(kvp.Key, kvp.Value);
                        }
                    }

                    // Add the current parsed schema. A top-level array, map or union has no
                    // name to register under, so there is nothing to add for one.
                    if (parsedSchema != null && !namedSchemas.Contains(parsedSchema.SchemaName))
                    {
                        namedSchemas.Add(parsedSchema.SchemaName, parsedSchema);
                    }
                }
            }

            return namedSchemas;
        }

        /// <summary>
        ///     Apache.Avro's *generic* writer type-checks a value against the schema and rejects
        ///     anything that is not a <see cref="Array" /> for an array schema ("Array required to
        ///     write against array schema but found ...List`1"). The shared
        ///     <c>Utils.TransformEnumerableAsync</c> builds a <c>List&lt;T&gt;</c>, which is what
        ///     the protobuf and JSON walks want and what an <c>ISpecificRecord</c> field typed
        ///     <c>IList&lt;T&gt;</c> requires - so the conversion belongs here, at the point the
        ///     value is assigned into a GenericRecord, and nowhere earlier. Converting in the walk
        ///     instead broke specific records with an <c>IList&lt;string&gt;</c> field.
        /// </summary>
        private static object ForGenericWriter(Avro.Schema schema, object value)
        {
            if (schema == null || value == null)
            {
                return value;
            }

            switch (schema.Tag)
            {
                case Avro.Schema.Type.Union:
                    // Resolve the branch the value takes, so a container inside a nullable union
                    // is normalised too.
                    foreach (Avro.Schema branch in ((UnionSchema)schema).Schemas)
                    {
                        if (branch.Tag != Avro.Schema.Type.Null && BranchAccepts(branch, value))
                        {
                            return ForGenericWriter(branch, value);
                        }
                    }

                    return value;

                case Avro.Schema.Type.Array:
                {
                    if (!(value is IList list))
                    {
                        return value;
                    }

                    // Recurse before converting: a nested array schema needs its inner
                    // collections to be arrays as well, and only the outermost one used to be
                    // converted.
                    Avro.Schema items = ((ArraySchema)schema).ItemSchema;
                    var array = new object[list.Count];
                    bool changed = !(value is Array);
                    for (int i = 0; i < list.Count; i++)
                    {
                        array[i] = ForGenericWriter(items, list[i]);
                        changed |= !ReferenceEquals(array[i], list[i]);
                    }

                    return changed ? array : value;
                }

                case Avro.Schema.Type.Map:
                {
                    if (!(value is IDictionary map))
                    {
                        return value;
                    }

                    // A map itself is accepted as an IDictionary; its values still have to be
                    // walked, because an array behind a map value has the same requirement.
                    Avro.Schema values = ((MapSchema)schema).ValueSchema;
                    var converted = new Dictionary<string, object>(map.Count);
                    bool valueChanged = false;
                    foreach (DictionaryEntry entry in map)
                    {
                        object item = ForGenericWriter(values, entry.Value);
                        valueChanged |= !ReferenceEquals(item, entry.Value);
                        converted[System.Convert.ToString(entry.Key)] = item;
                    }

                    return valueChanged ? (object)converted : value;
                }

                default:
                    return value;
            }
        }

        /// <summary>
        ///     Narrows the field context's descriptor to the slot being walked, so a leaf inside a
        ///     container sees its own schema rather than the container's.
        /// </summary>
        private static void SetSlotSchema(RuleContext ctx, Avro.Schema schema)
        {
            RuleContext.FieldContext fieldContext = ctx.CurrentField();
            if (fieldContext != null)
            {
                fieldContext.FieldDescriptor = schema;
            }
        }

        public static async Task<object> Transform(RuleContext ctx, Avro.Schema schema, object message,
            IFieldTransform fieldTransform)
        {
            // Only an absent schema stops the walk. A `null` *value* is the null branch of a
            // ["null", T] union and has to reach the rule: the reference binds it as CEL null so
            // a rule can guard with `value == null`, and returning early here skipped the rule
            // entirely - indistinguishable, to the caller, from a rule that ran and passed. The
            // reference guards a null only where there is nothing to walk, which is why the
            // array, map and record cases below each carry their own guard.
            if (schema == null)
            {
                return message;
            }

            RuleContext.FieldContext fieldContext = ctx.CurrentField();
            if (fieldContext != null)
            {
                fieldContext.Type = GetType(schema);
            }

            IUnionResolver writer;
            switch (schema.Tag)
            {
                case Avro.Schema.Type.Union:
                    writer = GetResolver(schema, message);
                    UnionSchema us = (UnionSchema)schema;
                    int unionIndex = writer.Resolve(us, message);
                    return await Transform(ctx, us[unionIndex], message, fieldTransform).ConfigureAwait(false);
                case Avro.Schema.Type.Array:
                    if (message == null)
                    {
                        return message;
                    }
                    ArraySchema a = (ArraySchema)schema;
                    // An element's slot is its own schema, not the array's.
                    SetSlotSchema(ctx, a.ItemSchema);
                    var arrayTransformer = (int index, object elem) =>
                        Transform(ctx, a.ItemSchema, elem, fieldTransform);
                    return await Utils.TransformEnumerableAsync(message, arrayTransformer).ConfigureAwait(false);
                case Avro.Schema.Type.Map:
                    if (message == null)
                    {
                        return message;
                    }
                    MapSchema ms = (MapSchema)schema;
                    // A value's slot is its own schema, not the map's.
                    SetSlotSchema(ctx, ms.ValueSchema);
                    var mapTransformer = (object key, object value) =>
                        Transform(ctx, ms.ValueSchema, value, fieldTransform);
                    return await Utils.TransformDictionaryAsync(message, mapTransformer).ConfigureAwait(false);
                case Avro.Schema.Type.Record:
                    if (message == null)
                    {
                        // A null record has no fields to walk.
                        return message;
                    }
                    RecordSchema rs = (RecordSchema)schema;
                    if (message is ISpecificRecord)
                    {
                        ISpecificRecord specificRecord = (ISpecificRecord)message;
                        rs = (RecordSchema)specificRecord.Schema;
                    }
                    else if (message is GenericRecord)
                    {
                        GenericRecord genericRecord = (GenericRecord)message;
                        rs = (RecordSchema)genericRecord.Schema;
                    }
                    foreach (Field f in rs.Fields)
                    {
                        Field originalField = null;
                        if (!((RecordSchema)schema).TryGetField(f.Name, out originalField))
                        {
                            originalField = f;
                        }

                        string fullName = rs.Fullname + "." + f.Name;
                        // The field's declared schema: a null branch's value carries no type, so
                        // the value alone cannot say what the field can hold.
                        using (ctx.EnterField(message, fullName, f.Name, GetType(originalField.Schema),
                                   GetInlineTags(originalField), originalField.Schema))
                        {
                            if (message is ISpecificRecord)
                            {
                                ISpecificRecord specificRecord = (ISpecificRecord)message;
                                object value = specificRecord.Get(f.Pos);
                                object newValue = await Transform(ctx, originalField.Schema, value, fieldTransform).ConfigureAwait(false);
                                if (ctx.Rule.Kind == RuleKind.Condition)
                                {
                                    if (newValue is bool b && !b)
                                    {
                                        throw new RuleConditionException(ctx.Rule);
                                    }
                                }
                                else
                                {
                                    specificRecord.Put(f.Pos, newValue);
                                }
                            }
                            else if (message is GenericRecord)
                            {
                                GenericRecord genericRecord = (GenericRecord)message;
                                object value = genericRecord.GetValue(f.Pos);
                                object newValue = await Transform(ctx, originalField.Schema, value, fieldTransform).ConfigureAwait(false);
                                if (ctx.Rule.Kind == RuleKind.Condition)
                                {
                                    if (newValue is bool b && !b)
                                    {
                                        throw new RuleConditionException(ctx.Rule);
                                    }
                                }
                                else
                                {
                                    genericRecord.Add(
                                        f.Pos, ForGenericWriter(originalField.Schema, newValue));
                                }
                            }
                            else
                            {
                                throw new ArgumentException("Unhandled field value of type " + message.GetType());
                            }
                        }
                    }

                    return message;
                default:
                    if (fieldContext != null)
                    {
                        ISet<string> ruleTags = ctx.Rule.Tags ?? new HashSet<string>();
                        ISet<string> intersect = new HashSet<string>(fieldContext.Tags);
                        intersect.IntersectWith(ruleTags);
                        
                        if (ruleTags.Count == 0 || intersect.Count != 0)
                        {
                            return await fieldTransform.Transform(ctx, fieldContext, message)
                                .ConfigureAwait(continueOnCapturedContext: false);
                        }
                    }

                    return message;
            }
        }

        /// <summary>
        ///     Walks the message against the schema, evaluating every inline
        ///     "confluent:rules" CHECK constraint encountered and collecting all failures.
        ///     Read-only — the message is not modified.
        ///
        ///     Two kinds of rules are evaluated:
        ///     <list type="bullet">
        ///       <item>Record-level ("confluent:rules" on a record schema) — <c>this</c> is
        ///         the record.</item>
        ///       <item>Field-level ("confluent:rules" on a record's field) — <c>this</c> is
        ///         the field value. Honors the skip-on-null contract: a field whose value is
        ///         null does not have its rules invoked.</item>
        ///     </list>
        ///
        ///     Failures are returned with their dotted-path location (e.g. addr.zip,
        ///     tags[3], scores["foo"]). The walk continues after each failure so callers see
        ///     the full set rather than only the first, unless failFast is set.
        /// </summary>
        public static async Task<IList<ValidationRuleError>> Validate(IValidationRuleExecutor executor,
            Avro.Schema schema, object message, bool failFast)
        {
            var violations = new List<ValidationRuleError>();
            if (executor == null || schema == null || message == null)
            {
                return violations;
            }

            await Validate(executor, schema, "", message, failFast, violations).ConfigureAwait(false);
            return violations;
        }

        /// <summary>
        ///     Mirrors <see cref="Transform" />'s switch-on-tag dispatch shape.
        /// </summary>
        private static async Task Validate(IValidationRuleExecutor executor, Avro.Schema schema,
            string path, object message, bool failFast, IList<ValidationRuleError> violations)
        {
            if (schema == null || message == null)
            {
                return;
            }

            switch (schema.Tag)
            {
                case Avro.Schema.Type.Union:
                    IUnionResolver writer = GetResolver(schema, message);
                    UnionSchema us = (UnionSchema)schema;
                    int unionIndex = writer.Resolve(us, message);
                    Avro.Schema member = us[unionIndex];
                    if (member.Tag == Avro.Schema.Type.Null)
                    {
                        return;
                    }

                    await Validate(executor, member, path, message, failFast, violations)
                        .ConfigureAwait(false);
                    return;
                case Avro.Schema.Type.Array:
                    ArraySchema a = (ArraySchema)schema;
                    if (message is IEnumerable array)
                    {
                        int index = 0;
                        foreach (object element in array)
                        {
                            await Validate(executor, a.ItemSchema, $"{path}[{index}]", element,
                                failFast, violations).ConfigureAwait(false);
                            if (failFast && violations.Any())
                            {
                                return;
                            }

                            index++;
                        }
                    }

                    return;
                case Avro.Schema.Type.Map:
                    MapSchema ms = (MapSchema)schema;
                    if (message is IDictionary map)
                    {
                        foreach (DictionaryEntry entry in map)
                        {
                            await Validate(executor, ms.ValueSchema, $"{path}[\"{entry.Key}\"]",
                                entry.Value, failFast, violations).ConfigureAwait(false);
                            if (failFast && violations.Any())
                            {
                                return;
                            }
                        }
                    }

                    return;
                case Avro.Schema.Type.Record:
                    RecordSchema declared = (RecordSchema)schema;
                    // Record-level rules: this = the record value.
                    foreach (ValidationRule rule in GetInlineValidationRules(declared))
                    {
                        await ValidationRules.Evaluate(executor, rule, declared, message, path, violations)
                            .ConfigureAwait(false);
                        if (failFast && violations.Any())
                        {
                            return;
                        }
                    }

                    // Iterate the runtime schema's fields (which handles schema evolution) but
                    // read inline metadata off the declared schema's field.
                    RecordSchema runtime = declared;
                    if (message is ISpecificRecord specific)
                    {
                        runtime = (RecordSchema)specific.Schema;
                    }
                    else if (message is GenericRecord generic)
                    {
                        runtime = (RecordSchema)generic.Schema;
                    }
                    else
                    {
                        return;
                    }

                    foreach (Field f in runtime.Fields)
                    {
                        if (!declared.TryGetField(f.Name, out Field originalField))
                        {
                            originalField = f;
                        }

                        object value = message is ISpecificRecord specificRecord
                            ? specificRecord.Get(f.Pos)
                            : ((GenericRecord)message).GetValue(f.Pos);
                        string childPath = path.Length == 0 ? f.Name : $"{path}.{f.Name}";

                        // Skip-on-null: a null field value does not invoke the executor. The
                        // recursion below still runs but no-ops for null.
                        if (value != null)
                        {
                            foreach (ValidationRule rule in GetInlineValidationRules(originalField))
                            {
                                await ValidationRules.Evaluate(executor, rule, originalField.Schema, value,
                                    childPath, violations).ConfigureAwait(false);
                                if (failFast && violations.Any())
                                {
                                    return;
                                }
                            }
                        }

                        await Validate(executor, originalField.Schema, childPath, value, failFast,
                            violations).ConfigureAwait(false);
                        if (failFast && violations.Any())
                        {
                            return;
                        }
                    }

                    return;
                default:
                    // primitive leaf — field-level rules were evaluated by the parent record case
                    return;
            }
        }

        private static IList<ValidationRule> GetInlineValidationRules(RecordSchema schema)
        {
            return ValidationRules.Parse(schema.GetProperty(ValidationRules.RulesProp));
        }

        private static IList<ValidationRule> GetInlineValidationRules(Field field)
        {
            return ValidationRules.Parse(field.GetProperty(ValidationRules.RulesProp));
        }

        private static RuleContext.Type GetType(Avro.Schema schema)
        {
            switch (schema.Tag)
            {
                case Avro.Schema.Type.Record:
                    return RuleContext.Type.Record;
                case Avro.Schema.Type.Enumeration:
                    return RuleContext.Type.Enum;
                case Avro.Schema.Type.Array:
                    return RuleContext.Type.Array;
                case Avro.Schema.Type.Map:
                    return RuleContext.Type.Map;
                case Avro.Schema.Type.Union:
                    return RuleContext.Type.Combined;
                case Avro.Schema.Type.Fixed:
                    return RuleContext.Type.Fixed;
                case Avro.Schema.Type.String:
                    return RuleContext.Type.String;
                case Avro.Schema.Type.Bytes:
                    return RuleContext.Type.Bytes;
                case Avro.Schema.Type.Int:
                    return RuleContext.Type.Int;
                case Avro.Schema.Type.Long:
                    return RuleContext.Type.Long;
                case Avro.Schema.Type.Float:
                    return RuleContext.Type.Float;
                case Avro.Schema.Type.Double:
                    return RuleContext.Type.Double;
                case Avro.Schema.Type.Boolean:
                    return RuleContext.Type.Boolean;
                case Avro.Schema.Type.Null:
                    return RuleContext.Type.Null;
                case Avro.Schema.Type.Logical:
                    return GetType((schema as LogicalSchema).BaseSchema);
                default:
                    return RuleContext.Type.Null;
            }
        }

        private static ISet<string> GetInlineTags(Field field)
        {
            String tagsProp = field.GetProperty("confluent:tags");
            if (tagsProp != null)
            {
                return JsonConvert.DeserializeObject<ISet<string>>(tagsProp);
            }
            return new HashSet<string>();
        }

        private static IUnionResolver GetResolver(Avro.Schema schema, object message)
        {
            if (message is ISpecificRecord)
            {
                return new AvroSpecificWriter(schema);
            }
            else
            {
                return new AvroGenericWriter(schema);
            }
        }

        private interface IUnionResolver
        {
            int Resolve(UnionSchema us, object obj);
        }

        private class AvroSpecificWriter : SpecificDefaultWriter, IUnionResolver
        {
            public AvroSpecificWriter(Avro.Schema schema) : base(schema)
            {
            }
            
            public int Resolve(UnionSchema us, object obj)
            {
                for (int i = 0; i < us.Count; i++)
                {
                    if (Matches(us[i], obj)) return i;
                }
                throw new AvroException("Cannot find a match for " + obj.GetType() + " in " + us);
            }
        }
        
        private class AvroGenericWriter : DefaultWriter, IUnionResolver
        {
            public AvroGenericWriter(Avro.Schema schema) : base(schema)
            {
            }
            
            public int Resolve(UnionSchema us, object obj)
            {
                for (int i = 0; i < us.Count; i++)
                {
                    if (Matches(us[i], obj)) return i;
                }
                throw new AvroException("Cannot find a match for " + obj.GetType() + " in " + us);
            }
        }
    }
}