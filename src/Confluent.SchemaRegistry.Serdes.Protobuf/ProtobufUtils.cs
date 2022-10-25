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
using Google.Protobuf;
using Google.Protobuf.Reflection;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     Protobuf utilities
    /// </summary>
    public static class ProtobufUtils
    {
        public static object Transform(RuleContext ctx, MessageDescriptor desc, object message,
            FieldTransform fieldTransform)
        {
            if (desc == null || message == null)
            {
                return message;
            }

            if (message is IList &&
                message.GetType().IsGenericType &&
                message.GetType().GetGenericTypeDefinition().IsAssignableFrom(typeof(List<>)))
            {
                List<object> list = (List<object>)message;
                return list.Select(it => Transform(ctx, desc, it, fieldTransform)).ToList();
            }
            else if (message is IDictionary &&
                     message.GetType().IsGenericType &&
                     message.GetType().GetGenericTypeDefinition().IsAssignableFrom(typeof(Dictionary<,>)))
            {
                return message;
            }
            else if (message is IMessage)
            {
                IMessage copy = Copy((IMessage)message);
                foreach (FieldDescriptor fd in copy.Descriptor.Fields.InDeclarationOrder())
                {
                    FieldDescriptor schemaFd = desc.FindFieldByName(fd.Name);
                    using (ctx.EnterField(ctx, copy, fd.FullName, fd.Name, GetType(fd), GetInlineAnnotations(fd)))
                    {
                        object value = fd.Accessor.GetValue(copy);
                        MessageDescriptor d = desc;
                        if (value is IMessage)
                        {
                            // Pass the schema-based descriptor which has the metadata
                            d = schemaFd.MessageType;
                        }

                        object newValue = Transform(ctx, d, value, fieldTransform);
                        fd.Accessor.SetValue(copy, newValue);
                    }
                }

                return copy;
            }
            else
            {
                RuleContext.FieldContext fieldContext = ctx.CurrentField();
                if (fieldContext != null)
                {
                    ISet<string> intersect = new HashSet<string>(fieldContext.Annotations);
                    intersect.IntersectWith(ctx.Rule.Annotations);
                    if (intersect.Count != 0)
                    {
                        return fieldTransform.Invoke(ctx, fieldContext, message);
                    }
                }

                return message;
            }
        }

        private static IMessage Copy(IMessage message)
        {
            var builder = (IMessage)Activator.CreateInstance(typeof(IMessage));
            builder.MergeFrom(message.ToByteArray());
            return builder;
        }

        private static RuleContext.Type GetType(FieldDescriptor field)
        {
            if (field.IsMap)
            {
                return RuleContext.Type.Map;
            }
            else if (field.IsRepeated)
            {
                return RuleContext.Type.Array;
            }

            switch (field.FieldType)
            {
                case FieldType.Message:
                    return RuleContext.Type.Record;
                case FieldType.Enum:
                    return RuleContext.Type.Enum;
                case FieldType.String:
                    return RuleContext.Type.String;
                case FieldType.Bytes:
                    return RuleContext.Type.Bytes;
                case FieldType.Int32:
                case FieldType.UInt32:
                case FieldType.Fixed32:
                case FieldType.SFixed32:
                    return RuleContext.Type.Int;
                case FieldType.Int64:
                case FieldType.UInt64:
                case FieldType.Fixed64:
                case FieldType.SFixed64:
                    return RuleContext.Type.Long;
                case FieldType.Float:
                    return RuleContext.Type.Float;
                case FieldType.Double:
                    return RuleContext.Type.Double;
                case FieldType.Bool:
                    return RuleContext.Type.Boolean;
                default:
                    return RuleContext.Type.Null;
            }
        }

        private static ISet<string> GetInlineAnnotations(FieldDescriptor fd)
        {
            ISet<string> annotations = new HashSet<string>();
            // TODO
            /*
            if (fd.getOptions().hasExtension(MetaProto.fieldMeta))
            {
                Meta meta = fd.getOptions().getExtension(MetaProto.fieldMeta);
                annotations.addAll(meta.getAnnotationList());
            }
            */
            return annotations;
        }
    }
}