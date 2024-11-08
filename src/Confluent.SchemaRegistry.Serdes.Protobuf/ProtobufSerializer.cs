// Copyright 2020 Confluent Inc.
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

// Disable obsolete warnings. ConstructValueSubjectName is still used a an internal implementation detail.
#pragma warning disable CS0618

extern alias ProtobufNet;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Confluent.Kafka;
using Google.Protobuf;
using ProtobufNet::Google.Protobuf.Reflection;
using FileDescriptor = Google.Protobuf.Reflection.FileDescriptor;
using MessageDescriptor = Google.Protobuf.Reflection.MessageDescriptor;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     Protobuf Serializer.
    /// </summary>
    /// <remarks>
    ///     Serialization format:
    ///       byte 0:           A magic byte that identifies this as a message with
    ///                         Confluent Platform framing.
    ///       bytes 1-4:        Unique global id of the Protobuf schema that was used
    ///                         for encoding (as registered in Confluent Schema Registry),
    ///                         big endian.
    ///       following bytes:  1. A size-prefixed array of indices that identify the
    ///                            specific message type in the schema (a given schema
    ///                            can contain many message types and they can be nested).
    ///                            Size and indices are unsigned varints. The common case
    ///                            where the message type is the first message in the
    ///                            schema (i.e. index data would be [1,0]) is encoded as
    ///                            a single 0 byte as an optimization.
    ///                         2. The protobuf serialized data.
    /// </remarks>
    public class ProtobufSerializer<T> : AsyncSerializer<T, FileDescriptorSet>  where T : IMessage<T>, new()
    {
        private bool skipKnownTypes;
        private bool useDeprecatedFormat;
        private ReferenceSubjectNameStrategyDelegate referenceSubjectNameStrategy;

        /// <remarks>
        ///     A given schema is uniquely identified by a schema id, even when
        ///     registered against multiple subjects.
        /// </remarks>
        private int? schemaId;

        private byte[] indexArray;


        /// <summary>
        ///     Initialize a new instance of the ProtobufSerializer class.
        /// </summary>
        public ProtobufSerializer(ISchemaRegistryClient schemaRegistryClient, ProtobufSerializerConfig config = null, 
            RuleRegistry ruleRegistry = null) : base(schemaRegistryClient, config, ruleRegistry)
        {
            if (config == null)
            { 
                this.referenceSubjectNameStrategy = ReferenceSubjectNameStrategy.ReferenceName.ToDelegate();
                return;
            }

            var nonProtobufConfig = config
                .Where(item => !item.Key.StartsWith("protobuf.") && !item.Key.StartsWith("rules."));
            if (nonProtobufConfig.Count() > 0)
            {
                throw new ArgumentException($"ProtobufSerializer: unknown configuration parameter {nonProtobufConfig.First().Key}");
            }

            if (config.BufferBytes != null) { this.initialBufferSize = config.BufferBytes.Value; }
            if (config.AutoRegisterSchemas != null) { this.autoRegisterSchema = config.AutoRegisterSchemas.Value; }
            if (config.NormalizeSchemas != null) { this.normalizeSchemas = config.NormalizeSchemas.Value; }
            if (config.UseLatestVersion != null) { this.useLatestVersion = config.UseLatestVersion.Value; }
            if (config.UseLatestWithMetadata != null) { this.useLatestWithMetadata = config.UseLatestWithMetadata; }
            if (config.SkipKnownTypes != null) { this.skipKnownTypes = config.SkipKnownTypes.Value; }
            if (config.UseDeprecatedFormat != null) { this.useDeprecatedFormat = config.UseDeprecatedFormat.Value; }
            if (config.SubjectNameStrategy != null) { this.subjectNameStrategy = config.SubjectNameStrategy.Value.ToDelegate(); }
            this.referenceSubjectNameStrategy = config.ReferenceSubjectNameStrategy == null
                ? ReferenceSubjectNameStrategy.ReferenceName.ToDelegate()
                : config.ReferenceSubjectNameStrategy.Value.ToDelegate();

            if (this.useLatestVersion && this.autoRegisterSchema)
            {
                throw new ArgumentException($"ProtobufSerializer: cannot enable both use.latest.version and auto.register.schemas");
            }
        }

        private static byte[] CreateIndexArray(MessageDescriptor md, bool useDeprecatedFormat)
        {
            var indices = new List<int>();

            // Walk the nested MessageDescriptor tree up to the root.
            var currentMd = md;
            while (currentMd.ContainingType != null)
            {
                var prevMd = currentMd;
                currentMd = currentMd.ContainingType;
                bool foundNested = false;
                for (int i=0; i<currentMd.NestedTypes.Count; ++i)
                {
                    if (currentMd.NestedTypes[i].ClrType == prevMd.ClrType)
                    {
                        indices.Add(i);
                        foundNested = true;
                        break;
                    }
                }
                if (!foundNested)
                {
                    throw new InvalidOperationException("Invalid message descriptor nesting.");
                }
            }

            // Add the index of the root MessageDescriptor in the FileDescriptor.
            bool foundDescriptor = false;
            for (int i=0; i<md.File.MessageTypes.Count; ++i)
            {
                if (md.File.MessageTypes[i].ClrType == currentMd.ClrType)
                {
                    indices.Add(i);
                    foundDescriptor = true;
                    break;
                }
            }
            if (!foundDescriptor)
            {
                throw new InvalidOperationException("MessageDescriptor not found.");
            }

            using (var result = new MemoryStream())
            {
                if (indices.Count == 1 && indices[0] == 0)
                {
                    // optimization for the special case [0]
                    result.WriteByte(0);
                }
                else
                {
                    if (useDeprecatedFormat)
                    {
                        result.WriteUnsignedVarint((uint)indices.Count);
                    }
                    else
                    {
                        result.WriteVarint((uint)indices.Count);
                    }
                    for (int i=0; i<indices.Count; ++i)
                    {
                        if (useDeprecatedFormat)
                        {
                            result.WriteUnsignedVarint((uint)indices[indices.Count-i-1]);
                        }
                        else
                        {
                            result.WriteVarint((uint)indices[indices.Count-i-1]);
                        }
                    }
                }

                return result.ToArray();
            }
        }


        /// <remarks>
        ///     note: protobuf does not support circular file references, so this possibility isn't considered.
        /// </remarks>
        private async Task<List<SchemaReference>> RegisterOrGetReferences(FileDescriptor fd, SerializationContext context, bool autoRegisterSchema, bool skipKnownTypes)
        {
            var tasks = new List<Task<SchemaReference>>();
            for (int i=0; i<fd.Dependencies.Count; ++i)
            {
                FileDescriptor fileDescriptor = fd.Dependencies[i];
                if (skipKnownTypes && fileDescriptor.Name.StartsWith("google/protobuf/"))
                {
                    continue;
                }
                
                Func<FileDescriptor, Task<SchemaReference>> t = async (dependency) => {
                    var dependencyReferences = await RegisterOrGetReferences(dependency, context, autoRegisterSchema, skipKnownTypes).ConfigureAwait(continueOnCapturedContext: false);
                    var subject = referenceSubjectNameStrategy(context, dependency.Name);
                    var schema = new Schema(dependency.SerializedData.ToBase64(), dependencyReferences, SchemaType.Protobuf);
                    var schemaId = autoRegisterSchema
                        ? await schemaRegistryClient.RegisterSchemaAsync(subject, schema, normalizeSchemas).ConfigureAwait(continueOnCapturedContext: false)
                        : await schemaRegistryClient.GetSchemaIdAsync(subject, schema, normalizeSchemas).ConfigureAwait(continueOnCapturedContext: false);
                    var registeredDependentSchema = await schemaRegistryClient.LookupSchemaAsync(subject, schema, true, normalizeSchemas).ConfigureAwait(continueOnCapturedContext: false);
                    return new SchemaReference(dependency.Name, subject, registeredDependentSchema.Version);
                };
                tasks.Add(t(fileDescriptor));
            }
            SchemaReference[] refs = await Task.WhenAll(tasks.ToArray()).ConfigureAwait(continueOnCapturedContext: false);

            return refs.ToList();
        }


        /// <summary>
        ///     Serialize an instance of type <typeparamref name="T"/> to a byte array
        ///     in Protobuf format. The serialized data is preceeded by:
        ///       1. A "magic byte" (1 byte) that identifies this as a message with
        ///          Confluent Platform framing.
        ///       2. The id of the schema as registered in Confluent's Schema Registry
        ///          (4 bytes, network byte order).
        ///       3. An size-prefixed array of indices that identify the specific message
        ///          type in the schema (a given schema can contain many message types
        ///          and they can be nested). Size and indices are unsigned varints. The
        ///          common case where the message type is the first message in the schema
        ///          (i.e. index data would be [1,0]) is encoded as simply a single 0 byte
        ///          as an optimization.
        ///     This call may block or throw on first use for a particular topic during
        ///     schema registration / verification.
        /// </summary>
        /// <param name="value">
        ///     The value to serialize.
        /// </param>
        /// <param name="context">
        ///     Context relevant to the serialize operation.
        /// </param>
        /// <returns>
        ///     A <see cref="System.Threading.Tasks.Task" /> that completes with 
        ///     <paramref name="value" /> serialized as a byte array.
        /// </returns>
        public override async Task<byte[]> SerializeAsync(T value, SerializationContext context)
        {
            if (value == null) { return null; }

            try
            {
                if (this.indexArray == null)
                {
                    this.indexArray = CreateIndexArray(value.Descriptor, useDeprecatedFormat);
                }

                string fullname = value.Descriptor.FullName;

                string subject;
                RegisteredSchema latestSchema = null;
                await serdeMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
                try
                {
                    subject = GetSubjectName(context.Topic, context.Component == MessageComponentType.Key, fullname);
                    latestSchema = await GetReaderSchema(subject)
                        .ConfigureAwait(continueOnCapturedContext: false);
                    
                    if (latestSchema != null)
                    {
                        schemaId = latestSchema.Id;
                    }
                    else if (!subjectsRegistered.Contains(subject))
                    {
                        var references =
                            await RegisterOrGetReferences(value.Descriptor.File, context, autoRegisterSchema, skipKnownTypes)
                                .ConfigureAwait(continueOnCapturedContext: false);

                        // first usage: register/get schema to check compatibility
                        schemaId = autoRegisterSchema
                            ? await schemaRegistryClient.RegisterSchemaAsync(subject,
                                    new Schema(value.Descriptor.File.SerializedData.ToBase64(), references,
                                        SchemaType.Protobuf), normalizeSchemas)
                                .ConfigureAwait(continueOnCapturedContext: false)
                            : await schemaRegistryClient.GetSchemaIdAsync(subject,
                                    new Schema(value.Descriptor.File.SerializedData.ToBase64(), references,
                                        SchemaType.Protobuf), normalizeSchemas)
                                .ConfigureAwait(continueOnCapturedContext: false);

                        // note: different values for schemaId should never be seen here.
                        // TODO: but fail fast may be better here.

                        subjectsRegistered.Add(subject);
                    }
                }
                finally
                {
                    serdeMutex.Release();
                }

                if (latestSchema != null)
                {
                    var fdSet = await GetParsedSchema(latestSchema).ConfigureAwait(false);
                    FieldTransformer fieldTransformer = async (ctx, transform, message) =>
                    {
                        return await ProtobufUtils.Transform(ctx, fdSet, message, transform).ConfigureAwait(false);
                    };
                    value = await ExecuteRules(context.Component == MessageComponentType.Key, subject,
                            context.Topic, context.Headers, RuleMode.Write, null,
                            latestSchema, value, fieldTransformer)
                        .ContinueWith(t => (T)t.Result).ConfigureAwait(continueOnCapturedContext: false);
                }

                using (var stream = new MemoryStream(initialBufferSize))
                using (var writer = new BinaryWriter(stream))
                {
                    stream.WriteByte(Constants.MagicByte);
                    writer.Write(IPAddress.HostToNetworkOrder(schemaId.Value));
                    writer.Write(this.indexArray);
                    value.WriteTo(stream);
                    return stream.ToArray();
                }
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        protected override async Task<FileDescriptorSet> ParseSchema(Schema schema)
        {
            IDictionary<string, string> references = await ResolveReferences(schema)
                .ConfigureAwait(continueOnCapturedContext: false);
            return ProtobufUtils.Parse(schema.SchemaString, references);
        }
    }
}
