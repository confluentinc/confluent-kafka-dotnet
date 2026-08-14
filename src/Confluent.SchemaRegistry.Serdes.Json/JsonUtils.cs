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
using System.Reflection;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NJsonSchema;
using NJsonSchema.Validation;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     JSON Schema utilities
    /// </summary>
    public static class JsonUtils
    {
        public static Task<object> Transform(RuleContext ctx, JsonSchema rootSchema, JsonSchema schema, string path, object message,
            IFieldTransform fieldTransform)
        {
            return Transform(ctx, rootSchema, schema, path, message, fieldTransform, null);
        }

        private static async Task<object> Transform(RuleContext ctx, JsonSchema rootSchema, JsonSchema schema, string path, object message,
            IFieldTransform fieldTransform, JsonObjectType? typeOverride)
        {
            if (schema == null || message == null)
            {
                return message;
            }

            // Use typeOverride if provided, otherwise use schema.Type (thread-safe read)
            JsonObjectType effectiveType = typeOverride ?? GetSchemaType(rootSchema, schema);

            RuleContext.FieldContext fieldContext = ctx.CurrentField();
            if (fieldContext != null)
            {
                fieldContext.Type = GetType(effectiveType);
            }

            // Only enter this block if effectiveType has multiple flags (needs resolution)
            // If typeOverride was provided, effectiveType is a single flag, so this is skipped
            if (HasMultipleFlags(effectiveType))
            {
                JToken jsonObject = JToken.FromObject(message);
                foreach (JsonObjectType flag in Enum.GetValues(typeof(JsonObjectType)))
                {
                    if (effectiveType.HasFlag(flag) && !flag.Equals(default(JsonObjectType)))
                    {
                        // Check if this type flag matches the message, with lock on root schema to protect mutation
                        bool isValid;
                        lock (rootSchema)
                        {
                            JsonObjectType originalType = schema.Type;
                            try
                            {
                                schema.Type = flag;
                                var validator = new JsonSchemaValidator();
                                var errors = validator.Validate(jsonObject, schema);
                                isValid = errors.Count == 0;
                            }
                            finally
                            {
                                schema.Type = originalType;
                            }
                        }

                        if (isValid)
                        {
                            // Pass flag as typeOverride - recursive call uses resolved type
                            return await Transform(ctx, rootSchema, schema, path, message,
                                fieldTransform, flag).ConfigureAwait(false);
                        }
                    }
                }
            }
            if (schema.AllOf.Count > 0 || schema.AnyOf.Count > 0 || schema.OneOf.Count > 0)
            {
                if (schema.AllOf.Count > 0)
                {
                    foreach (JsonSchema subschema in schema.AllOf)
                    {
                        message = await Transform(ctx, rootSchema, subschema, path, message, fieldTransform, null).ConfigureAwait(false);
                    }
                }
                else
                {
                    ICollection<JsonSchema> subschemas = schema.AnyOf.Count > 0 ? schema.AnyOf : schema.OneOf;
                    bool oneOf = schema.OneOf.Count > 0;
                    JToken jsonObject = JToken.FromObject(message);
                    foreach (JsonSchema subschema in subschemas)
                    {
                        bool isValid;
                        lock (rootSchema)
                        {
                            var validator = new JsonSchemaValidator();
                            var errors = validator.Validate(jsonObject, subschema);
                            isValid = errors.Count == 0;
                        }
                        if (isValid)
                        {
                            // New subschema, no type override needed
                            message = await Transform(ctx, rootSchema, subschema, path, message, fieldTransform, null).ConfigureAwait(false);
                            if (oneOf)
                            {
                                break;
                            }
                        }
                    }
                }

                // Also visit sibling properties/items at this level (NJsonSchema keeps them
                // on the same schema object alongside allOf/anyOf/oneOf).
                if (schema.Properties.Count > 0)
                {
                    message = await TransformProperties(ctx, rootSchema, schema, path, message, fieldTransform).ConfigureAwait(false);
                }
                if (schema.Item != null && message is IList)
                {
                    JsonSchema itemSchema = schema.Item;
                    var transformer = (int index, object elem) =>
                        Transform(ctx, rootSchema, itemSchema, path + '[' + index + ']', elem, fieldTransform, null);
                    message = await Utils.TransformEnumerableAsync(message, transformer).ConfigureAwait(false);
                }

                return message;
            }
            else if (effectiveType.HasFlag(JsonObjectType.Array))
            {
                bool isList = typeof(IList).IsAssignableFrom(message.GetType())
                              || (message.GetType().IsGenericType
                                  && (message.GetType().GetGenericTypeDefinition() == typeof(List<>)
                                      || message.GetType().GetGenericTypeDefinition() == typeof(IList<>)));
                if (!isList)
                {
                    return message;
                }

                JsonSchema subschema = schema.Item;
                var transformer = (int index, object elem) =>
                    Transform(ctx, rootSchema, subschema, path + '[' + index + ']', elem, fieldTransform, null);
                return await Utils.TransformEnumerableAsync(message, transformer).ConfigureAwait(false);
            }
            else if (effectiveType.HasFlag(JsonObjectType.Object) || schema.Properties.Count > 0)
            {
                return await TransformProperties(ctx, rootSchema, schema, path, message, fieldTransform).ConfigureAwait(false);
            }
            else if (schema.HasReference)
            {
                // Follow reference, no type override needed
                return await Transform(ctx, rootSchema, schema.ActualTypeSchema, path, message, fieldTransform, null).ConfigureAwait(false);
            }
            else
            {
                fieldContext = ctx.CurrentField();
                if (fieldContext != null)
                {
                    switch (effectiveType)
                    {
                        case JsonObjectType.Boolean:
                        case JsonObjectType.Integer:
                        case JsonObjectType.Number:
                        case JsonObjectType.String:
                            ISet<string> ruleTags = ctx.Rule.Tags ?? new HashSet<string>();
                            ISet<string> intersect = new HashSet<string>(fieldContext.Tags);
                            intersect.IntersectWith(ruleTags);

                            if (ruleTags.Count == 0 || intersect.Count != 0)
                            {
                                return await fieldTransform.Transform(ctx, fieldContext, message)
                                    .ConfigureAwait(continueOnCapturedContext: false);
                            }
                            break;
                        case JsonObjectType.Null:
                        default:
                            break;
                    }
                }

                return message;
            }
        }

        private static async Task<object> TransformProperties(RuleContext ctx, JsonSchema rootSchema, JsonSchema schema,
            string path, object message, IFieldTransform fieldTransform)
        {
            if (message is JObject jObject)
            {
                foreach (var it in schema.Properties)
                {
                    if (!jObject.TryGetValue(it.Key, out JToken token))
                    {
                        continue;
                    }
                    string fullName = path + '.' + it.Key;
                    using (ctx.EnterField(message, fullName, it.Key, GetType(rootSchema, it.Value), GetInlineTags(it.Value)))
                    {
                        object value = token is JValue jv ? jv.Value : (object)token;
                        object newValue = await Transform(ctx, rootSchema, it.Value, fullName, value, fieldTransform, null).ConfigureAwait(false);
                        if (ctx.Rule.Kind == RuleKind.Condition)
                        {
                            if (newValue is bool b && !b)
                            {
                                throw new RuleConditionException(ctx.Rule);
                            }
                        }
                        else
                        {
                            jObject[it.Key] = newValue == null
                                ? JValue.CreateNull()
                                : (newValue as JToken ?? JToken.FromObject(newValue));
                        }
                    }
                }
                return message;
            }

            foreach (var it in schema.Properties)
            {
                string fullName = path + '.' + it.Key;
                using (ctx.EnterField(message, fullName, it.Key, GetType(rootSchema, it.Value), GetInlineTags(it.Value)))
                {
                    FieldAccessor fieldAccessor;
                    try
                    {
                        fieldAccessor = FieldAccessorCache.GetOrAdd(
                            (message.GetType(), it.Key),
                            key => new FieldAccessor(key.Item1, key.Item2));
                    }
                    catch (ArgumentException)
                    {
                        continue;
                    }
                    object value = fieldAccessor.GetFieldValue(message);
                    // New field schema, no type override needed
                    object newValue = await Transform(ctx, rootSchema, it.Value, fullName, value, fieldTransform, null).ConfigureAwait(false);
                    if (ctx.Rule.Kind == RuleKind.Condition)
                    {
                        if (newValue is bool b && !b)
                        {
                            throw new RuleConditionException(ctx.Rule);
                        }
                    }
                    else
                    {
                        fieldAccessor.SetFieldValue(message, newValue);
                    }
                }
            }
            return message;
        }

        /// <summary>
        ///     Walks the message against the schema, evaluating every inline
        ///     "confluent:rules" constraint encountered and collecting all failures.
        ///     Read-only — the message is not modified.
        ///
        ///     Two kinds of rules are evaluated:
        ///     <list type="bullet">
        ///       <item>Object-level ("confluent:rules" on an object schema) — <c>this</c> is
        ///         the object.</item>
        ///       <item>Property-level ("confluent:rules" on a property schema) — <c>this</c>
        ///         is the property value. Honors the skip-on-null contract: a property that
        ///         is absent or null does not have its rules invoked.</item>
        ///     </list>
        ///
        ///     Failures are returned with their location, rooted at "$" to match the JVM
        ///     client (e.g. $.addr.zip, $.tags[3]). The walk continues after each failure
        ///     unless failFast is set.
        /// </summary>
        public static async Task<IList<ValidationRuleError>> Validate(IValidationRuleExecutor executor,
            JsonSchema rootSchema, object message, bool failFast)
        {
            var violations = new List<ValidationRuleError>();
            if (executor == null || rootSchema == null || message == null)
            {
                return violations;
            }

            await Validate(executor, rootSchema, rootSchema, "$", message, failFast, violations)
                .ConfigureAwait(false);
            return violations;
        }

        /// <summary>
        ///     Mirrors <see cref="Transform" />'s dispatch shape: the combined keywords
        ///     (allOf/anyOf/oneOf) with their sibling properties/items, then arrays, then
        ///     objects, then references.
        /// </summary>
        private static async Task Validate(IValidationRuleExecutor executor, JsonSchema rootSchema,
            JsonSchema schema, string path, object message, bool failFast,
            IList<ValidationRuleError> violations, JsonObjectType? typeOverride = null)
        {
            if (schema == null || message == null)
            {
                return;
            }

            // Rules declared at this level: this = the value at this location. This is the
            // only place rules are read - a property's schema and the schema the walk
            // recurses into for that property are the same object, so reading them in the
            // property loop as well would charge every rule on an object-valued property
            // twice. The message == null guard above is also the skip-on-null contract.
            // Matches the JVM client.
            foreach (ValidationRule rule in GetInlineValidationRules(schema))
            {
                await ValidationRules.Evaluate(executor, rule, schema, message, path, violations)
                    .ConfigureAwait(false);
                if (failFast && violations.Any())
                {
                    return;
                }
            }

            JsonObjectType effectiveType = typeOverride ?? GetSchemaType(rootSchema, schema);

            // A schema whose type allows several kinds has to be narrowed to the one the
            // value actually is before dispatching: JsonObjectType is a flag set, so
            // ["array","object"] carries the Array flag, and an object value would enter the
            // array branch, fail its IList check and return with the object's own property
            // rules never visited. Transform resolves the type the same way.
            if (typeOverride == null && HasMultipleFlags(effectiveType))
            {
                JToken jsonObject = JToken.FromObject(message);
                foreach (JsonObjectType flag in Enum.GetValues(typeof(JsonObjectType)))
                {
                    if (!effectiveType.HasFlag(flag) || flag.Equals(default(JsonObjectType)))
                    {
                        continue;
                    }

                    bool isValid;
                    lock (rootSchema)
                    {
                        JsonObjectType originalType = schema.Type;
                        try
                        {
                            schema.Type = flag;
                            var validator = new JsonSchemaValidator();
                            isValid = validator.Validate(jsonObject, schema).Count == 0;
                        }
                        finally
                        {
                            schema.Type = originalType;
                        }
                    }

                    if (isValid)
                    {
                        // The rules for this node have already been evaluated above, so the
                        // resolved pass must not read them again.
                        await ValidateResolved(executor, rootSchema, schema, path, message,
                            failFast, violations, flag).ConfigureAwait(false);
                        return;
                    }
                }
            }

            await ValidateResolved(executor, rootSchema, schema, path, message, failFast,
                violations, effectiveType).ConfigureAwait(false);
        }

        /// <summary>
        ///     Walks into whatever the schema describes, with its type already resolved to a
        ///     single kind. The rules for this node have been evaluated by
        ///     <see cref="Validate" />.
        /// </summary>
        private static async Task ValidateResolved(IValidationRuleExecutor executor,
            JsonSchema rootSchema, JsonSchema schema, string path, object message,
            bool failFast, IList<ValidationRuleError> violations, JsonObjectType effectiveType)
        {
            if (schema.AllOf.Count > 0 || schema.AnyOf.Count > 0 || schema.OneOf.Count > 0)
            {
                if (schema.AllOf.Count > 0)
                {
                    foreach (JsonSchema subschema in schema.AllOf)
                    {
                        await Validate(executor, rootSchema, subschema, path, message, failFast, violations)
                            .ConfigureAwait(false);
                        if (failFast && violations.Any())
                        {
                            return;
                        }
                    }
                }
                else
                {
                    ICollection<JsonSchema> subschemas = schema.AnyOf.Count > 0 ? schema.AnyOf : schema.OneOf;
                    bool oneOf = schema.OneOf.Count > 0;
                    JToken jsonObject = JToken.FromObject(message);
                    foreach (JsonSchema subschema in subschemas)
                    {
                        bool isValid;
                        lock (rootSchema)
                        {
                            var validator = new JsonSchemaValidator();
                            isValid = validator.Validate(jsonObject, subschema).Count == 0;
                        }

                        if (isValid)
                        {
                            await Validate(executor, rootSchema, subschema, path, message, failFast, violations)
                                .ConfigureAwait(false);
                            if (oneOf || (failFast && violations.Any()))
                            {
                                break;
                            }
                        }
                    }
                }

                if (failFast && violations.Any())
                {
                    return;
                }

                // Also visit sibling properties/items at this level.
                await ValidateProperties(executor, rootSchema, schema, path, message, failFast, violations)
                    .ConfigureAwait(false);
                if (failFast && violations.Any())
                {
                    return;
                }

                if (schema.Item != null && message is IList)
                {
                    await ValidateArray(executor, rootSchema, schema.Item, path, message, failFast, violations)
                        .ConfigureAwait(false);
                }

                return;
            }

            if (effectiveType.HasFlag(JsonObjectType.Array))
            {
                if (message is IList)
                {
                    await ValidateArray(executor, rootSchema, schema.Item, path, message, failFast, violations)
                        .ConfigureAwait(false);
                }

                return;
            }

            if (effectiveType.HasFlag(JsonObjectType.Object) || schema.Properties.Count > 0)
            {
                await ValidateProperties(executor, rootSchema, schema, path, message, failFast, violations)
                    .ConfigureAwait(false);
                return;
            }

            if (schema.HasReference)
            {
                await Validate(executor, rootSchema, schema.ActualTypeSchema, path, message, failFast,
                    violations).ConfigureAwait(false);
            }

            // otherwise a primitive leaf - its rules were evaluated above, and it has no children
        }

        private static async Task ValidateArray(IValidationRuleExecutor executor, JsonSchema rootSchema,
            JsonSchema itemSchema, string path, object message, bool failFast,
            IList<ValidationRuleError> violations)
        {
            if (itemSchema == null || !(message is IList list))
            {
                return;
            }

            for (int i = 0; i < list.Count; i++)
            {
                await Validate(executor, rootSchema, itemSchema, $"{path}[{i}]", list[i], failFast,
                    violations).ConfigureAwait(false);
                if (failFast && violations.Any())
                {
                    return;
                }
            }
        }

        /// <summary>
        ///     Recurses into each declared property value. Undeclared properties are not
        ///     walked, matching the JVM client. Rules are not read here - see Validate,
        ///     which each property value goes through.
        /// </summary>
        private static async Task ValidateProperties(IValidationRuleExecutor executor, JsonSchema rootSchema,
            JsonSchema schema, string path, object message, bool failFast,
            IList<ValidationRuleError> violations)
        {
            foreach (var it in schema.Properties)
            {
                string fullName = path + '.' + it.Key;
                object value;
                if (message is JObject jObject)
                {
                    if (!jObject.TryGetValue(it.Key, out JToken token))
                    {
                        continue;
                    }

                    value = token is JValue jv ? jv.Value : (object)token;
                }
                else
                {
                    FieldAccessor fieldAccessor;
                    try
                    {
                        fieldAccessor = FieldAccessorCache.GetOrAdd(
                            (message.GetType(), it.Key),
                            key => new FieldAccessor(key.Item1, key.Item2));
                    }
                    catch (ArgumentException)
                    {
                        continue;
                    }

                    value = fieldAccessor.GetFieldValue(message);
                }

                await Validate(executor, rootSchema, it.Value, fullName, value, failFast, violations)
                    .ConfigureAwait(false);
                if (failFast && violations.Any())
                {
                    return;
                }
            }
        }

        /// <summary>
        ///     Reads the "confluent:rules" keyword off a schema. NJsonSchema preserves
        ///     unknown keywords in ExtensionData, so this is a plain lookup.
        /// </summary>
        private static IList<ValidationRule> GetInlineValidationRules(JsonSchema schema)
        {
            if (schema?.ExtensionData == null
                || !schema.ExtensionData.TryGetValue(ValidationRules.RulesProp, out var prop)
                || prop == null)
            {
                return new List<ValidationRule>();
            }

            return ValidationRules.Parse(JsonConvert.SerializeObject(prop));
        }

        private static bool HasMultipleFlags<T>(T flags) where T : Enum
        {
            var value = Convert.ToInt32(flags);
            return value != 0 && (value & (value - 1)) != 0;
        }

        /// <summary>
        ///     Thread-safe accessor for schema.Type.
        ///     Prevents reading temporarily mutated values during concurrent type validation.
        ///     Locks on rootSchema to ensure consistency across the entire schema tree.
        /// </summary>
        private static JsonObjectType GetSchemaType(JsonSchema rootSchema, JsonSchema schema)
        {
            lock (rootSchema)
            {
                return schema.Type;
            }
        }

        private static RuleContext.Type GetType(JsonSchema rootSchema, JsonSchema schema)
        {
            return GetType(GetSchemaType(rootSchema, schema));
        }

        private static RuleContext.Type GetType(JsonObjectType type)
        {
            switch (type)
            {
                case JsonObjectType.Object:
                    return RuleContext.Type.Record;
                case JsonObjectType.Array:
                    return RuleContext.Type.Array;
                case JsonObjectType.String:
                    return RuleContext.Type.String;
                case JsonObjectType.Integer:
                    return RuleContext.Type.Int;
                case JsonObjectType.Number:
                    return RuleContext.Type.Double;
                case JsonObjectType.Boolean:
                    return RuleContext.Type.Boolean;
                case JsonObjectType.Null:
                default:
                    return RuleContext.Type.Null;
            }
        }

        private static ISet<string> GetInlineTags(JsonSchema schema)
        {
            if (schema.ExtensionData != null && schema.ExtensionData.TryGetValue("confluent:tags", out var tagsProp))
            {
                if (tagsProp is object[] tags)
                {
                    return new HashSet<string>(tags.Select(x => x.ToString()).ToList());
                }
            }
            return new HashSet<string>();
        }

        private static readonly ConcurrentDictionary<(Type, string), FieldAccessor> FieldAccessorCache = new();

        class FieldAccessor
        {
            protected Func<object, object> GetValue { get; }
            protected Action<object, object> SetValue { get; }

            public FieldAccessor(Type type, string fieldName)
            {
                var propertyInfo = type.GetProperty(fieldName,
                    BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
                if (propertyInfo != null)
                {
                    GetValue = value => propertyInfo.GetValue(value);
                    SetValue = (instance, value) => propertyInfo.SetValue(instance, value);
                    return;
                }
                
                var fieldInfo = type.GetField(fieldName,
                    BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
                if (fieldInfo != null)
                {
                    GetValue = value => fieldInfo.GetValue(value);
                    SetValue = (instance, value) => fieldInfo.SetValue(instance, value);
                    return;
                }
                
                foreach (PropertyInfo prop in type.GetProperties())
                {
                    if (prop.IsDefined(typeof(JsonPropertyAttribute)))
                    {
                        var attrs = prop.GetCustomAttributes(typeof(JsonPropertyAttribute), true);
                        foreach (JsonPropertyAttribute attr in attrs)
                        {
                            if (attr.PropertyName.Equals(fieldName))
                            {
                                GetValue = value => prop.GetValue(value);
                                SetValue = (instance, value) => prop.SetValue(instance, value);
                                return;
                            }
                        }
                    }
                }
                
                foreach (FieldInfo field in type.GetFields())
                {
                    if (field.IsDefined(typeof(JsonPropertyAttribute)))
                    {
                        var attrs = field.GetCustomAttributes(typeof(JsonPropertyAttribute), true);
                        foreach (JsonPropertyAttribute attr in attrs)
                        {
                            if (attr.PropertyName.Equals(fieldName))
                            {
                                GetValue = value => field.GetValue(value);
                                SetValue = (instance, value) => field.SetValue(instance, value);
                                return;
                            }
                        }
                    }
                }
                
                throw new ArgumentException("Could not find field " + fieldName);
            }

            public object GetFieldValue(object message)
            {
                return GetValue(message);
            }

            public void SetFieldValue(object message, object value)
            {
                SetValue(message, value);
            }
        }

        internal static ICollection<ValidationError> FlattenPropertyValidationErrors(
            IEnumerable<ValidationError> validationResult,
            ICollection<ValidationError>? flattenedErrors = null,
            int depth = 0)
        {
            flattenedErrors ??= new List<ValidationError>();
            const int maxDepth = 32;

            if (validationResult is null) return flattenedErrors;

            foreach (var error in validationResult)
            {
                if (error is null) continue;

                if (error is ChildSchemaValidationError child && depth < maxDepth)
                {
                    foreach (var nested in child.Errors?.Values ?? Enumerable.Empty<ICollection<ValidationError>>())
                    {
                        if (nested is null || nested.Count == 0) continue;
                        FlattenPropertyValidationErrors(nested, flattenedErrors, depth + 1);
                    }
                }
                else
                {
                    flattenedErrors.Add(error);
                }
            }

            return flattenedErrors;
        }
    }
}