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
        // Lock to protect schema.Type mutation during validation of union types
        private static readonly object SchemaTypeLock = new object();

        public static async Task<object> Transform(RuleContext ctx, JsonSchema schema, string path, object message,
            IFieldTransform fieldTransform)
        {
            if (schema == null || message == null)
            {
                return message;
            }

            RuleContext.FieldContext fieldContext = ctx.CurrentField();
            if (fieldContext != null)
            {
                fieldContext.Type = GetType(schema);
            }

            if (HasMultipleFlags(schema.Type))
            {
                JToken jsonObject = JToken.FromObject(message);
                foreach (JsonObjectType flag in Enum.GetValues(typeof(JsonObjectType)))
                {
                    if (schema.Type.HasFlag(flag) && !flag.Equals(default(JsonObjectType)))
                    {
                        // Check if this type flag matches the message, with lock to protect schema mutation
                        bool isValid;
                        lock (SchemaTypeLock)
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
                            return await Transform(ctx, schema, path, message,
                                fieldTransform).ConfigureAwait(false);
                        }
                    }
                }
            }
            if (schema.AllOf.Count > 0 || schema.AnyOf.Count > 0 || schema.OneOf.Count > 0)
            {
                JToken jsonObject = JToken.FromObject(message);
                ICollection<JsonSchema> subschemas;
                if (schema.AllOf.Count > 0)
                {
                    subschemas = schema.AllOf;
                }
                else if (schema.AnyOf.Count > 0)
                {
                    subschemas = schema.AnyOf;
                }
                else
                {
                    subschemas = schema.OneOf;
                }
                foreach (JsonSchema subschema in subschemas)
                {
                    var validator = new JsonSchemaValidator();
                    var errors = validator.Validate(jsonObject, subschema);
                    if (errors.Count == 0)
                    {
                        return await Transform(ctx, subschema, path, message, fieldTransform).ConfigureAwait(false);
                    }
                }

                return message;
            }
            else if (schema.IsArray)
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
                    Transform(ctx, subschema, path + '[' + index + ']', elem, fieldTransform);
                return await Utils.TransformEnumerableAsync(message, transformer).ConfigureAwait(false);
            }
            else if (schema.IsObject || schema.Properties.Count > 0)
            {
                foreach (var it in schema.Properties)
                {
                    string fullName = path + '.' + it.Key;
                    using (ctx.EnterField(message, fullName, it.Key, GetType(it.Value), GetInlineTags(it.Value)))
                    {
                        FieldAccessor fieldAccessor;
                        try
                        {
                            fieldAccessor = new FieldAccessor(message.GetType(), it.Key);
                        }
                        catch (ArgumentException)
                        {
                            continue;
                        }
                        object value = fieldAccessor.GetFieldValue(message);
                        object newValue = await Transform(ctx, it.Value, fullName, value, fieldTransform).ConfigureAwait(false);
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
            else if (schema.HasReference)
            {
                return await Transform(ctx, schema.ActualTypeSchema, path, message, fieldTransform).ConfigureAwait(false);
            }
            else
            {
                fieldContext = ctx.CurrentField();
                if (fieldContext != null)
                {
                    switch (schema.Type)
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

        private static bool HasMultipleFlags<T>(T flags) where T : Enum
        {
            var value = Convert.ToInt32(flags);
            return value != 0 && (value & (value - 1)) != 0;
        }

        private static RuleContext.Type GetType(JsonSchema schema)
        {
            return GetType(schema.Type);
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
    }
}