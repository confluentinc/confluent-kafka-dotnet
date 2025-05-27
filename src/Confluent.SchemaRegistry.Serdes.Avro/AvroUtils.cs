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
        public static async Task<object> Transform(RuleContext ctx, Avro.Schema schema, object message,
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

            IUnionResolver writer;
            switch (schema.Tag)
            {
                case Avro.Schema.Type.Union:
                    writer = GetResolver(schema, message);
                    UnionSchema us = (UnionSchema)schema;
                    int unionIndex = writer.Resolve(us, message);
                    return await Transform(ctx, us[unionIndex], message, fieldTransform).ConfigureAwait(false);
                case Avro.Schema.Type.Array:
                    ArraySchema a = (ArraySchema)schema;
                    var arrayTransformer = (int index, object elem) =>
                        Transform(ctx, a.ItemSchema, elem, fieldTransform);
                    return await Utils.TransformEnumerableAsync(message, arrayTransformer).ConfigureAwait(false);
                case Avro.Schema.Type.Map:
                    MapSchema ms = (MapSchema)schema;
                    var mapTransformer = (object key, object value) =>
                        Transform(ctx, ms.ValueSchema, value, fieldTransform);
                    return await Utils.TransformDictionaryAsync(message, mapTransformer).ConfigureAwait(false);
                case Avro.Schema.Type.Record:
                    RecordSchema rs = (RecordSchema)schema;
                    foreach (Field f in rs.Fields)
                    {
                        string fullName = rs.Fullname + "." + f.Name;
                        using (ctx.EnterField(message, fullName, f.Name, GetType(f.Schema), GetInlineTags(f)))
                        {
                            if (message is ISpecificRecord)
                            {
                                ISpecificRecord specificRecord = (ISpecificRecord)message;
                                object value = specificRecord.Get(f.Pos);
                                object newValue = await Transform(ctx, f.Schema, value, fieldTransform).ConfigureAwait(false);
                                if (ctx.Rule.Kind == RuleKind.Condition)
                                {
                                    if (newValue is bool b && !b)
                                    {
                                        throw new RuleConditionException(ctx.Rule);
                                    }
                                } else
                                {
                                    specificRecord.Put(f.Pos, newValue);
                                }
                            }
                            else if (message is GenericRecord)
                            {
                                GenericRecord genericRecord = (GenericRecord)message;
                                object value = genericRecord.GetValue(f.Pos);
                                object newValue = await Transform(ctx, f.Schema, value, fieldTransform).ConfigureAwait(false);
                                if (ctx.Rule.Kind == RuleKind.Condition)
                                {
                                    if (newValue is bool b && !b)
                                    {
                                        throw new RuleConditionException(ctx.Rule);
                                    }
                                }
                                else
                                {
                                    genericRecord.Add(f.Pos, newValue);
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