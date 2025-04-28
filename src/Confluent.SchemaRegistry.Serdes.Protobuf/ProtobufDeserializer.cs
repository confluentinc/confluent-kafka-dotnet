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

extern alias ProtobufNet;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Net;
using Confluent.Kafka;
using Google.Protobuf;
using ProtobufNet::Google.Protobuf.Reflection;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     (async) Protobuf deserializer.
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
    public class ProtobufDeserializer<T> : AsyncDeserializer<T, FileDescriptorSet> where T : class, IMessage<T>, new()
    {
        private bool useDeprecatedFormat;
        
        private MessageParser<T> parser;

        /// <summary>
        ///     Initialize a new ProtobufDeserializer instance.
        /// </summary>
        /// <param name="config">
        ///     Deserializer configuration properties (refer to 
        ///     <see cref="ProtobufDeserializerConfig" />).
        /// </param>
        public ProtobufDeserializer(IEnumerable<KeyValuePair<string, string>> config = null) : this(null, config)
        {
        }

        public ProtobufDeserializer(ISchemaRegistryClient schemaRegistryClient, IEnumerable<KeyValuePair<string, string>> config = null) 
            : this(schemaRegistryClient, config != null ? new ProtobufDeserializerConfig(config) : null)
        {
        }

        public ProtobufDeserializer(ISchemaRegistryClient schemaRegistryClient, ProtobufDeserializerConfig config, 
            RuleRegistry ruleRegistry = null) : base(schemaRegistryClient, config, ruleRegistry)
        {
            this.parser = new MessageParser<T>(() => new T());

            if (config == null) { return; }

            var nonProtobufConfig = config
                .Where(item => !item.Key.StartsWith("protobuf.") && !item.Key.StartsWith("rules."));
            if (nonProtobufConfig.Count() > 0)
            {
                throw new ArgumentException($"ProtobufDeserializer: unknown configuration parameter {nonProtobufConfig.First().Key}");
            }

            ProtobufDeserializerConfig protobufConfig = new ProtobufDeserializerConfig(config);
            if (protobufConfig.UseDeprecatedFormat != null)
            {
                this.useDeprecatedFormat = protobufConfig.UseDeprecatedFormat.Value;
            }

            if (config.UseLatestVersion != null) { this.useLatestVersion = config.UseLatestVersion.Value; }
            if (config.UseLatestWithMetadata != null) { this.useLatestWithMetadata = config.UseLatestWithMetadata; }
            if (config.SubjectNameStrategy != null) { this.subjectNameStrategy = config.SubjectNameStrategy.Value.ToDelegate(); }
            if (config.SchemaIdStrategy != null) { this.schemaIdDeserializer = config.SchemaIdStrategy.Value.ToDeserializer(); }
        }

        /// <summary>
        ///     Deserialize an object of type <typeparamref name="T"/>
        ///     from a byte array.
        /// </summary>
        /// <param name="data">
        ///     The raw byte data to deserialize.
        /// </param>
        /// <param name="isNull">
        ///     True if this is a null value.
        /// </param>
        /// <param name="context">
        ///     Context relevant to the deserialize operation.
        /// </param>
        /// <returns>
        ///     A <see cref="System.Threading.Tasks.Task" /> that completes
        ///     with the deserialized value.
        /// </returns>
        public override async Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull) { return null; }

            var array = data.ToArray();
            if (array.Length < 6)
            {
                throw new InvalidDataException($"Expecting data framing of length 6 bytes or more but total data size is {array.Length} bytes");
            }

            bool isKey = context.Component == MessageComponentType.Key;
            string topic = context.Topic;
            string subject = GetSubjectName(topic, isKey, null);

            // Currently Protobuf does not support migration rules because of lack of support for DynamicMessage
            // See https://github.com/protocolbuffers/protobuf/issues/658
            /*
            RegisteredSchema latestSchema = await SerdeUtils.GetReaderSchema(schemaRegistryClient, subject, useLatestWithMetadata, useLatestVersion)
                .ConfigureAwait(continueOnCapturedContext: false);
            */

            try
            {
                Schema writerSchema = null;
                FileDescriptorSet fdSet = null;
                T message;
                SchemaId writerId = new SchemaId(SchemaType.Protobuf);
                using (var stream = schemaIdDeserializer.Deserialize(array, context, writerId))
                {
                    if (schemaRegistryClient != null)
                    {
                        (writerSchema, fdSet) = await GetSchema(subject, writerId);
                    }

                    message = parser.ParseFrom(stream);
                }

                if (writerSchema != null)
                {
                    FieldTransformer fieldTransformer = async (ctx, transform, messageToTransform) =>
                    {
                        return await ProtobufUtils.Transform(ctx, fdSet, messageToTransform, transform).ConfigureAwait(false);
                    };
                    message = await ExecuteRules(context.Component == MessageComponentType.Key, subject, context.Topic, context.Headers, RuleMode.Read, null,
                        writerSchema, message, fieldTransformer)
                        .ContinueWith(t => (T)t.Result)
                        .ConfigureAwait(continueOnCapturedContext: false);
                }
                
                return message;
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
