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

extern alias ProtobufNet;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using ProtobufNet::ProtoBuf.Reflection;
using IFileSystem = ProtobufNet::Google.Protobuf.Reflection.IFileSystem;
using FileDescriptorSet = ProtobufNet::Google.Protobuf.Reflection.FileDescriptorSet;
using DescriptorProto = ProtobufNet::Google.Protobuf.Reflection.DescriptorProto;
using FieldDescriptorProto = ProtobufNet::Google.Protobuf.Reflection.FieldDescriptorProto;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///   Protobuf utilities (internal utils for processing protobuf resources)
    /// </summary>
    internal static class ProtobufUtils
    {
        private static IDictionary<string, string> BuiltIns = new Dictionary<string, string>
        {
            { "confluent/meta.proto", GetResource("confluent.meta.proto") },
            { "confluent/type/decimal.proto", GetResource("confluent.type.decimal.proto") },
            { "google/type/calendar_period.proto", GetResource("google.type.calendar_period.proto") },
            { "google/type/color.proto", GetResource("google.type.color.proto") },
            { "google/type/date.proto", GetResource("google.type.date.proto") },
            { "google/type/datetime.proto", GetResource("google.type.datetime.proto") },
            { "google/type/dayofweek.proto", GetResource("google.type.dayofweek.proto") },
            { "google/type/expr.proto", GetResource("google.type.expr.proto") },
            { "google/type/fraction.proto", GetResource("google.type.fraction.proto") },
            { "google/type/latlng.proto", GetResource("google.type.latlng.proto") },
            { "google/type/money.proto", GetResource("google.type.money.proto") },
            { "google/type/month.proto", GetResource("google.type.month.proto") },
            { "google/type/postal_address.proto", GetResource("google.type.postal_address.proto") },
            { "google/type/quaternion.proto", GetResource("google.type.quaternion.proto") },
            { "google/type/timeofday.proto", GetResource("google.type.timeofday.proto") },
            { "google/protobuf/any.proto", GetResource("google.protobuf.any.proto") },
            { "google/protobuf/api.proto", GetResource("google.protobuf.api.proto") },
            { "google/protobuf/descriptor.proto", GetResource("google.protobuf.descriptor.proto") },
            { "google/protobuf/duration.proto", GetResource("google.protobuf.duration.proto") },
            { "google/protobuf/empty.proto", GetResource("google.protobuf.empty.proto") },
            { "google/protobuf/field_mask.proto", GetResource("google.protobuf.field_mask.proto") },
            { "google/protobuf/source_context.proto", GetResource("google.protobuf.source_context.proto") },
            { "google/protobuf/struct.proto", GetResource("google.protobuf.struct.proto") },
            { "google/protobuf/timestamp.proto", GetResource("google.protobuf.timestamp.proto") },
            { "google/protobuf/type.proto", GetResource("google.protobuf.type.proto") },
            { "google/protobuf/wrappers.proto", GetResource("google.protobuf.wrappers.proto") }
        }.ToImmutableDictionary();

        private static string GetResource(string resourceName)
        {
            var info = Assembly.GetExecutingAssembly().GetName();
            var name = info.Name;
            using (var stream = Assembly
                .GetExecutingAssembly()
                .GetManifestResourceStream($"{name}.proto.{resourceName}"))
            {
                using (var streamReader = new StreamReader(stream, Encoding.UTF8))
                {
                    return streamReader.ReadToEnd();
                }
            }
        }

        internal static async Task<object> Transform(RuleContext ctx, object desc, object message,
            IFieldTransform fieldTransform)
        {
            if (desc == null || message == null)
            {
                return message;
            }

            RuleContext.FieldContext fieldContext = ctx.CurrentField();

            if (typeof(IList).IsAssignableFrom(message.GetType())
                || (message.GetType().IsGenericType
                    && (message.GetType().GetGenericTypeDefinition() == typeof(List<>)
                        || message.GetType().GetGenericTypeDefinition() == typeof(IList<>))))
            {
                var transformer = (int index, object elem) =>
                    Transform(ctx, desc, elem, fieldTransform);
                return await Utils.TransformEnumerableAsync(message, transformer).ConfigureAwait(false);
            }
            else if (typeof(IDictionary).IsAssignableFrom(message.GetType())
                     || (message.GetType().IsGenericType
                         && (message.GetType().GetGenericTypeDefinition() == typeof(Dictionary<,>)
                             || message.GetType().GetGenericTypeDefinition() == typeof(IDictionary<,>))))
            {
                return message;
            }
            else if (message is IMessage)
            {
                IMessage copy = Copy((IMessage)message);
                string messageFullName = copy.Descriptor.FullName;
                if (!messageFullName.StartsWith("."))
                {
                    messageFullName = "." + messageFullName;
                }

                DescriptorProto messageType = FindMessageByName(desc, messageFullName);
                foreach (FieldDescriptor fd in copy.Descriptor.Fields.InDeclarationOrder())
                {
                    FieldDescriptorProto schemaFd = FindFieldByName(messageType, fd.Name);
                    using (ctx.EnterField(copy, fd.FullName, fd.Name, GetType(fd), GetInlineTags(schemaFd)))
                    {
                        if (fd.ContainingOneof != null && !fd.Accessor.HasValue(copy)) {
                            // Skip oneof fields that are not set
                            continue;
                        }
                        object value = fd.Accessor.GetValue(copy);
                        DescriptorProto d = messageType;
                        if (value is IMessage)
                        {
                            // Pass the schema-based descriptor which has the metadata
                            d = schemaFd.GetMessageType();
                        }

                        object newValue = await Transform(ctx, d, value, fieldTransform).ConfigureAwait(false);
                        if (ctx.Rule.Kind == RuleKind.Condition)
                        {
                            if (newValue is bool b && !b)
                            {
                                throw new RuleConditionException(ctx.Rule);
                            }
                        }
                        else
                        {
                            fd.Accessor.SetValue(copy, newValue);
                        }
                    }
                }

                return copy;
            }
            else
            {
                if (fieldContext != null)
                {
                    ISet<string> ruleTags = ctx.Rule.Tags ?? new HashSet<string>();
                    ISet<string> intersect = new HashSet<string>(fieldContext.Tags);
                    intersect.IntersectWith(ruleTags);

                    if (ruleTags.Count == 0 || intersect.Count != 0)
                    {
                        if (message is ByteString)
                        {
                            message = ((ByteString)message).ToByteArray();
                        }
                        message = await fieldTransform.Transform(ctx, fieldContext, message)
                            .ConfigureAwait(continueOnCapturedContext: false);
                        if (message is byte[])
                        {
                            message = ByteString.CopyFrom((byte[])message);
                        }

                        return message;
                    }
                }

                return message;
            }
        }

        private static DescriptorProto FindMessageByName(object desc, string messageFullName)
        {
            if (desc is FileDescriptorSet)
            {
                foreach (var file in ((FileDescriptorSet)desc).Files)
                {
                    foreach (var messageType in file.MessageTypes)
                    {
                        DescriptorProto found = FindMessageByName(messageType, messageFullName);
                        if (found != null)
                        {
                            return found;
                        }
                    }
                }
            }
            else if (desc is DescriptorProto)
            {
                DescriptorProto messageType = (DescriptorProto)desc;
                if (messageType.GetFullyQualifiedName().Equals(messageFullName))
                {
                    return messageType;
                }

                foreach (DescriptorProto nestedType in messageType.NestedTypes)
                {
                    DescriptorProto found = FindMessageByName(nestedType, messageFullName);
                    if (found != null)
                    {
                        return found;
                    }
                }
            }
            return null;
        }

        private static FieldDescriptorProto FindFieldByName(DescriptorProto desc, string fieldName)
        {
            foreach (FieldDescriptorProto fd in desc.Fields)
            {
                if (fd.Name.Equals(fieldName))
                {
                    return fd;
                }
            }

            return null;
        }

        private static IMessage Copy(IMessage message)
        {
            var builder = (IMessage)Activator.CreateInstance(message.GetType());
            builder.MergeFrom(message.ToByteArray());
            return builder;
        }

        private static RuleContext.Type GetType(FieldDescriptor field)
        {
            if (field.IsMap)
            {
                return RuleContext.Type.Map;
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
                case FieldType.SInt32:
                case FieldType.UInt32:
                case FieldType.Fixed32:
                case FieldType.SFixed32:
                    return RuleContext.Type.Int;
                case FieldType.Int64:
                case FieldType.SInt64:
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

        /// <summary>
        ///     Walks the message against the descriptor, evaluating every inline validation
        ///     rule declared in the confluent.Meta extension and collecting all failures.
        ///     Read-only — the message is not modified.
        ///
        ///     Two kinds of rules are evaluated:
        ///     <list type="bullet">
        ///       <item>Message-level (confluent.message_meta rules) — <c>this</c> is the
        ///         message.</item>
        ///       <item>Field-level (confluent.field_meta rules) — <c>this</c> is the field
        ///         value; for repeated and map fields that is the whole collection. Honors
        ///         the skip-on-null contract: an unset oneof member does not have its rules
        ///         invoked.</item>
        ///     </list>
        ///
        ///     Failures are returned with their dotted-path location (e.g. addr.zip,
        ///     items[3], labels["k"]). The walk continues after each failure unless failFast
        ///     is set.
        ///
        ///     Only message_meta and field_meta rules are evaluated; rules on files, enums
        ///     and enum values are ignored, matching the JVM client.
        /// </summary>
        internal static async Task<IList<ValidationRuleError>> Validate(IValidationRuleExecutor executor,
            object desc, object message, bool failFast)
        {
            var violations = new List<ValidationRuleError>();
            if (executor == null || desc == null || message == null)
            {
                return violations;
            }

            await Validate(executor, desc, "", message, failFast, violations).ConfigureAwait(false);
            return violations;
        }

        /// <summary>
        ///     Mirrors <see cref="Transform" />'s dispatch shape, walking the descriptor's
        ///     fields and descending into message-valued fields, map values and repeated
        ///     elements.
        /// </summary>
        private static async Task Validate(IValidationRuleExecutor executor, object desc, string path,
            object message, bool failFast, IList<ValidationRuleError> violations)
        {
            if (desc == null || !(message is IMessage protoMessage))
            {
                return;
            }

            string messageFullName = protoMessage.Descriptor.FullName;
            if (!messageFullName.StartsWith("."))
            {
                messageFullName = "." + messageFullName;
            }

            DescriptorProto messageType = FindMessageByName(desc, messageFullName);
            if (messageType == null)
            {
                return;
            }

            // Message-level rules: this = the message.
            foreach (ValidationRule rule in GetInlineValidationRules(GetMeta(messageType.Options)))
            {
                await ValidationRules.Evaluate(executor, rule, messageType, protoMessage, path, violations)
                    .ConfigureAwait(false);
                if (failFast && violations.Any())
                {
                    return;
                }
            }

            foreach (FieldDescriptor fd in protoMessage.Descriptor.Fields.InDeclarationOrder())
            {
                FieldDescriptorProto schemaFd = FindFieldByName(messageType, fd.Name);
                if (schemaFd == null)
                {
                    continue;
                }

                // Skip-on-null: an unset oneof member does not invoke the executor.
                if (fd.ContainingOneof != null && !fd.Accessor.HasValue(protoMessage))
                {
                    continue;
                }

                object value = fd.Accessor.GetValue(protoMessage);
                if (value == null)
                {
                    continue;
                }

                string childPath = path.Length == 0 ? fd.Name : $"{path}.{fd.Name}";
                foreach (ValidationRule rule in GetInlineValidationRules(GetMeta(schemaFd.Options)))
                {
                    await ValidationRules.Evaluate(executor, rule, schemaFd, value, childPath, violations)
                        .ConfigureAwait(false);
                    if (failFast && violations.Any())
                    {
                        return;
                    }
                }

                if (fd.IsMap)
                {
                    if (value is IDictionary map)
                    {
                        foreach (DictionaryEntry entry in map)
                        {
                            if (!(entry.Value is IMessage))
                            {
                                continue;
                            }

                            await Validate(executor, schemaFd.GetMessageType(),
                                $"{childPath}[\"{entry.Key}\"]", entry.Value, failFast, violations)
                                .ConfigureAwait(false);
                            if (failFast && violations.Any())
                            {
                                return;
                            }
                        }
                    }
                }
                else if (fd.IsRepeated)
                {
                    if (value is IList list)
                    {
                        for (int i = 0; i < list.Count; i++)
                        {
                            if (!(list[i] is IMessage))
                            {
                                continue;
                            }

                            await Validate(executor, schemaFd.GetMessageType(), $"{childPath}[{i}]",
                                list[i], failFast, violations).ConfigureAwait(false);
                            if (failFast && violations.Any())
                            {
                                return;
                            }
                        }
                    }
                }
                else if (value is IMessage)
                {
                    await Validate(executor, schemaFd.GetMessageType(), childPath, value, failFast,
                        violations).ConfigureAwait(false);
                    if (failFast && violations.Any())
                    {
                        return;
                    }
                }
            }
        }

        private static IList<ValidationRule> GetInlineValidationRules(
            global::Confluent.SchemaRegistry.Serdes.Protobuf.Meta meta)
        {
            if (meta == null || meta.Rules.Count == 0)
            {
                return new List<ValidationRule>();
            }

            return meta.Rules
                .Select(r => new ValidationRule
                {
                    Name = r.Name,
                    Doc = r.Doc,
                    Expr = r.Expr,
                    Sql = r.Sql
                })
                .ToList();
        }

        /// <summary>
        ///     The field number of the confluent.Meta extension on descriptor options.
        /// </summary>
        private const int MetaFieldNumber = 1088;

        /// <summary>
        ///     Reads the confluent.Meta option off a descriptor's options, or null when the
        ///     option is absent.
        ///
        ///     protobuf-net resolves the option into an extension holding the serialized
        ///     Meta message, so it never shows up in UninterpretedOptions; the bytes are
        ///     read back out of the extension and parsed with the generated Meta type.
        /// </summary>
        internal static global::Confluent.SchemaRegistry.Serdes.Protobuf.Meta GetMeta(
            global::ProtoBuf.IExtensible options)
        {
            if (options == null)
            {
                return null;
            }

            var extension = options.GetExtensionObject(false);
            if (extension == null)
            {
                return null;
            }

            Stream stream = extension.BeginQuery();
            try
            {
                var input = new CodedInputStream(stream);
                uint tag;
                while ((tag = input.ReadTag()) != 0)
                {
                    // Wire type 2 (length-delimited) carries the embedded Meta message.
                    if ((int)(tag >> 3) == MetaFieldNumber && (tag & 7) == 2)
                    {
                        return global::Confluent.SchemaRegistry.Serdes.Protobuf.Meta.Parser
                            .ParseFrom(input.ReadBytes());
                    }

                    input.SkipLastField();
                }
            }
            catch (InvalidProtocolBufferException)
            {
                return null;
            }
            finally
            {
                extension.EndQuery(stream);
            }

            return null;
        }

        private static ISet<string> GetInlineTags(FieldDescriptorProto fd)
        {
            var meta = GetMeta(fd.Options);
            return meta == null ? new HashSet<string>() : new HashSet<string>(meta.Tags);
        }

        internal static FileDescriptorSet Parse(string schema, IDictionary<string, string> imports)
        {
            var fds = new FileDescriptorSet
            {
                FileSystem = new ProtobufImports(imports)
            };
            fds.Add("__root.proto", true, new StringReader(schema));
            fds.AddImportPath(""); // all imports are relative in the filesystem so must make import path just empty string
            fds.Process();
            return fds;
        }

        private class ProtobufImports : IFileSystem
        {
            private readonly IDictionary<string, string> imports;

            public ProtobufImports(IDictionary<string, string> imports)
            {
                this.imports = imports;
            }

            public bool Exists(string path)
            {
                return BuiltIns.ContainsKey(path) || (imports?.ContainsKey(path) ?? false);
            }

            public TextReader OpenText(string path)
            {
                return new StringReader(BuiltIns.TryGetValue(path, out var res) ? res : imports[path]);
            }
        }
    }
}
