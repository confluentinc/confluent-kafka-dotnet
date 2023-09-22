// Copyright 2020-2023 Confluent Inc.
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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using NJsonSchema;
using NJsonSchema.Generation;
using NJsonSchema.Validation;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     (async) JSON deserializer.
    /// </summary>
    /// <remarks>
    ///     Serialization format:
    ///       byte 0:           A magic byte that identifies this as a message with
    ///                         Confluent Platform framing.
    ///       bytes 1-4:        Unique global id of the JSON schema associated with
    ///                         the data (as registered in Confluent Schema Registry),
    ///                         big endian.
    ///       following bytes:  The JSON data (utf8)
    ///
    ///     Internally, uses Newtonsoft.Json for deserialization. Currently,
    ///     no explicit validation of the data is done against the
    ///     schema stored in Schema Registry.
    ///
    ///     Note: Off-the-shelf libraries do not yet exist to enable
    ///     integration of System.Text.Json and JSON Schema, so this
    ///     is not yet supported by the deserializer.
    /// </remarks>
    public class JsonDeserializer<T> : IAsyncDeserializer<T> where T : class
    {
        private readonly int headerSize = sizeof(int) + sizeof(byte);

        private readonly JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings;
        private JsonSchema schema = null;
        private ISchemaRegistryClient schemaRegistryClient;
        
        /// <summary>
        ///     Initialize a new JsonDeserializer instance
        ///     with a given Schema.
        /// </summary>
        /// <param name="schemaRegistryClient">
        ///     Confluent Schema Registry client instance.
        /// </param>
        /// <param name="schema">
        ///     Schema to use for validation, used when external
        ///     schema references are present in the schema. 
        ///     Populate the References list of the schema for
        ///     the same. Assuming the referenced schemas have
        ///     already been registered in the registry.
        /// </param>
        /// <param name="config">
        ///     Deserializer configuration properties (refer to
        ///     <see cref="JsonDeserializerConfig" />).
        /// </param>
        /// <param name="jsonSchemaGeneratorSettings">
        ///     JSON schema generator settings.
        /// </param>
        public JsonDeserializer(ISchemaRegistryClient schemaRegistryClient, Schema schema, IEnumerable<KeyValuePair<string, string>> config = null,
            JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null)
        {
            this.schemaRegistryClient = schemaRegistryClient;
            this.jsonSchemaGeneratorSettings = jsonSchemaGeneratorSettings;

            JsonSchemaResolver utils = new JsonSchemaResolver(
                schemaRegistryClient, schema, this.jsonSchemaGeneratorSettings);
            JsonSchema jsonSchema = utils.GetResolvedSchema();
            this.schema = jsonSchema;

            if (config == null) { return; }

            if (config.Count() > 0)
            {
                throw new ArgumentException($"JsonDeserializer: unknown configuration parameter {config.First().Key}.");
            }
        }

        /// <summary>
        ///     Initialize a new JsonDeserializer instance.
        /// </summary>
        /// <param name="config">
        ///     Deserializer configuration properties (refer to
        ///     <see cref="JsonDeserializerConfig" />).
        /// </param>
        /// <param name="jsonSchemaGeneratorSettings">
        ///     JSON schema generator settings.
        /// </param>
        public JsonDeserializer(IEnumerable<KeyValuePair<string, string>> config = null, JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null)
        {
            this.jsonSchemaGeneratorSettings = jsonSchemaGeneratorSettings;

            if (config == null) { return; }

            if (config.Count() > 0)
            {
                throw new ArgumentException($"JsonDeserializer: unknown configuration parameter {config.First().Key}.");
            }
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
        public Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull) { return Task.FromResult<T>(null); }

            try
            {
                var array = data.ToArray();

                if (array.Length < 5)
                {
                    throw new InvalidDataException($"Expecting data framing of length 5 bytes or more but total data size is {array.Length} bytes");
                }

                if (array[0] != Constants.MagicByte)
                {
                    throw new InvalidDataException($"Expecting message {context.Component.ToString()} with Confluent Schema Registry framing. Magic byte was {array[0]}, expecting {Constants.MagicByte}");
                }

                // A schema is not required to deserialize json messages.
                using (var stream = new MemoryStream(array, headerSize, array.Length - headerSize))
                using (var sr = new System.IO.StreamReader(stream, Encoding.UTF8))
                {
                    string serializedString = sr.ReadToEnd();
                    
                    if (this.schema != null)
                    {
                        JsonSchemaValidator validator = new JsonSchemaValidator();
                        var validationResult = validator.Validate(serializedString, this.schema);

                        if (validationResult.Count > 0)
                        {
                            throw new InvalidDataException("Schema validation failed for properties: [" + string.Join(", ", validationResult.Select(r => r.Path)) + "]");
                        }
                    }
                    return Task.FromResult(Newtonsoft.Json.JsonConvert.DeserializeObject<T>(serializedString, this.jsonSchemaGeneratorSettings?.ActualSerializerSettings));
                }
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }
    }
}
