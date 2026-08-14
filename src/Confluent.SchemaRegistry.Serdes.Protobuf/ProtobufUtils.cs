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
using FileDescriptorProto = ProtobufNet::Google.Protobuf.Reflection.FileDescriptorProto;
using PbnSerializer = ProtobufNet::ProtoBuf.Serializer;


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
                    FieldDescriptorProto schemaFd = FindFieldByNumber(messageType, fd.FieldNumber);
                    if (schemaFd == null)
                    {
                        // The schema does not declare this field, so it carries no tags.
                        continue;
                    }

                    // The names come from the registered schema alongside the tags: rules
                    // and metadata tags are written against it. The value is still read
                    // through the runtime field.
                    string schemaFieldName = schemaFd.Name;
                    string schemaFullName = FieldFullName(messageType, schemaFd);
                    using (ctx.EnterField(copy, schemaFullName, schemaFieldName, GetType(fd),
                        GetInlineTags(schemaFd)))
                    {
                        // Skip-on-null: a field that tracks presence and is unset does not
                        // invoke the executor. HasPresence covers a oneof member, an
                        // `optional` scalar and a singular message field alike, which is the
                        // predicate the Java, Go and C++ clients use. It has to be tested
                        // first: HasValue throws for a field with no presence to report.
                        if (fd.HasPresence && !fd.Accessor.HasValue(copy)) {
                            continue;
                        }
                        object value = fd.Accessor.GetValue(copy);
                        object newValue;
                        if (fd.IsMap)
                        {
                            // A map's values are descended into with the value type's own
                            // descriptor, the same way the validation walk descends into
                            // them; a map of scalars has nothing below it to descend into.
                            DescriptorProto valueType = GetMapValueMessageType(schemaFd);
                            newValue = valueType == null
                                ? value
                                : await TransformMapValues(ctx, valueType, value, fieldTransform)
                                    .ConfigureAwait(false);
                        }
                        else
                        {
                            // Descend with the field's own message type - for a repeated
                            // field's elements as much as for a singular message. Walking
                            // them against the containing descriptor looks their fields up
                            // in the wrong message, which is also how the schema-based
                            // metadata is found.
                            DescriptorProto d = messageType;
                            if (IsMessageKind(schemaFd))
                            {
                                d = schemaFd.GetMessageType() ?? messageType;
                            }

                            newValue = await Transform(ctx, d, value, fieldTransform)
                                .ConfigureAwait(false);
                        }

                        if (ctx.Rule.Kind == RuleKind.Condition)
                        {
                            if (newValue is bool b && !b)
                            {
                                throw new RuleConditionException(ctx.Rule);
                            }
                        }
                        else if (fd.IsMap)
                        {
                            // The map was updated through the live collection; a repeated
                            // or map field cannot be assigned.
                        }
                        else if (fd.IsRepeated)
                        {
                            if (value is IList target && newValue is IList transformed
                                && !ReferenceEquals(target, transformed))
                            {
                                target.Clear();
                                foreach (object element in transformed)
                                {
                                    target.Add(element);
                                }
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

        /// <summary>
        ///     Whether a field holds a message, so that the walks descend into it.
        /// </summary>
        private static bool IsMessageKind(FieldDescriptorProto schemaFd) =>
            schemaFd.type == FieldDescriptorProto.Type.TypeMessage
            || schemaFd.type == FieldDescriptorProto.Type.TypeGroup;

        /// <summary>
        ///     The descriptor of a map field's value type, or null when the values are
        ///     scalars. A map field's own message type is its entry type, whose fields are
        ///     the key and the value - not the descriptor either walk needs, since both
        ///     descend into the values themselves.
        /// </summary>
        private static DescriptorProto GetMapValueMessageType(FieldDescriptorProto schemaFd)
        {
            DescriptorProto entryType = schemaFd.GetMessageType();
            FieldDescriptorProto valueFd = entryType == null
                ? null
                : entryType.Fields.FirstOrDefault(field => field.Name == "value");
            return valueFd != null && IsMessageKind(valueFd) ? valueFd.GetMessageType() : null;
        }

        /// <summary>
        ///     Transforms every value of a map in place, descending into each with
        ///     <paramref name="valueType" />.
        /// </summary>
        private static async Task<object> TransformMapValues(RuleContext ctx,
            DescriptorProto valueType, object value, IFieldTransform fieldTransform)
        {
            if (!(value is IDictionary map))
            {
                return value;
            }

            // Collected first: a map cannot be written to while it is being enumerated.
            var updates = new List<KeyValuePair<object, object>>();
            foreach (DictionaryEntry entry in map)
            {
                object newValue = await Transform(ctx, valueType, entry.Value, fieldTransform)
                    .ConfigureAwait(false);
                updates.Add(new KeyValuePair<object, object>(entry.Key, newValue));
            }

            foreach (KeyValuePair<object, object> update in updates)
            {
                map[update.Key] = update.Value;
            }

            return map;
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

        /// <summary>
        ///     The fully qualified name of a schema-side field, which protobuf-net's
        ///     FieldDescriptorProto does not carry on its own.
        /// </summary>
        private static string FieldFullName(DescriptorProto messageType, FieldDescriptorProto fd)
        {
            string messageName = messageType.GetFullyQualifiedName();
            if (messageName.StartsWith("."))
            {
                messageName = messageName.Substring(1);
            }

            return messageName + "." + fd.Name;
        }

        /// <summary>
        ///     Finds a field by number. Protobuf identifies a field by its number, and
        ///     renaming a field at the same number is a compatible change, so with
        ///     use.latest.version the registered schema's name for a field can differ from
        ///     the message's - resolving by name would find nothing and silently skip the
        ///     field's rules and tags.
        /// </summary>
        private static FieldDescriptorProto FindFieldByNumber(DescriptorProto desc, int fieldNumber)
        {
            if (desc == null)
            {
                return null;
            }

            foreach (FieldDescriptorProto fd in desc.Fields)
            {
                if (fd.Number == fieldNumber)
                {
                    return fd;
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
                FieldDescriptorProto schemaFd = FindFieldByNumber(messageType, fd.FieldNumber);
                if (schemaFd == null)
                {
                    continue;
                }

                // Skip-on-null: a field that tracks presence and is unset does not invoke
                // the executor. HasPresence covers a oneof member, an `optional` scalar and a
                // singular message field alike, which is the predicate the Java, Go and C++
                // clients use. It has to be tested first: HasValue throws for a field with no
                // presence to report.
                if (fd.HasPresence && !fd.Accessor.HasValue(protoMessage))
                {
                    continue;
                }

                object value = fd.Accessor.GetValue(protoMessage);
                if (value == null)
                {
                    continue;
                }

                // Paths and names come from the registered schema, which is what a rule
                // refers to.
                string childPath = path.Length == 0 ? schemaFd.Name : $"{path}.{schemaFd.Name}";
                foreach (ValidationRule rule in GetInlineValidationRules(GetMeta(schemaFd.Options)))
                {
                    // The rules come from the registered schema, but the type hint is the
                    // runtime field: it is what describes the value actually in hand, and it
                    // settles distinctions the CLR type cannot - an enum from an int, a
                    // uint64 from an int64.
                    await ValidationRules.Evaluate(executor, rule, fd, value, childPath, violations)
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

                            await Validate(executor, GetMapValueMessageType(schemaFd),
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

        /// <summary>
        ///     Builds the protobuf-net descriptor set the rule walkers need from a
        ///     compiled-in <see cref="FileDescriptor"/>, for the paths where no schema text
        ///     is available from the registry.
        ///
        ///     protobuf-net's descriptor types are protobuf messages over the same wire
        ///     format as <see cref="FileDescriptor.SerializedData"/>, so the descriptors can
        ///     be loaded directly rather than round-tripped through .proto text. Custom
        ///     options survive as extension data, which is what <see cref="GetMeta"/> reads.
        /// </summary>
        internal static FileDescriptorSet ParseFromDescriptor(FileDescriptor fileDescriptor)
        {
            var set = new FileDescriptorSet();
            var visited = new HashSet<string>();
            AddFileWithDependencies(set, fileDescriptor, visited);
            // Process() resolves the fully-qualified names and field type names the walkers
            // navigate by. It reports unresolved extendees for the well-known option types,
            // which is harmless here: the walkers read the raw extension bytes rather than
            // asking protobuf-net to interpret the options.
            set.Process();
            return set;
        }

        private static void AddFileWithDependencies(FileDescriptorSet set,
            FileDescriptor fileDescriptor, ISet<string> visited)
        {
            if (fileDescriptor == null || !visited.Add(fileDescriptor.Name))
            {
                return;
            }

            foreach (FileDescriptor dependency in fileDescriptor.Dependencies)
            {
                AddFileWithDependencies(set, dependency, visited);
            }

            using (var stream = new MemoryStream(fileDescriptor.SerializedData.ToByteArray()))
            {
                set.Files.Add(PbnSerializer.Deserialize<FileDescriptorProto>(stream));
            }
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
