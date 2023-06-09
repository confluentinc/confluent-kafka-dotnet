// Copyright 2023 Confluent Inc.
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
using NJsonSchema.Generation;

namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     Reference Based Json Serializer.
    /// </summary>
    /// <remarks>
    ///     Does the same tasks as JsonSerializer class, this is to be used when
    ///     the user wants to pass a Schema object to the serializer instead of a 
    ///     class. Generally to be used when schema has external references.
    ///     Note: It requires ProducerBuilder(string,object) for creating the producer
    ///     as it is equivalent to JsonSerializer(object).
    /// </remarks>
    public class ReferenceSchemaBasedJsonSerializer : JsonSerializer<object>
    {
        /// <summary>
        ///     Initialize a new instance of the ReferenceSchemaBasedJsonSerializer class.
        /// </summary>
        /// <param name="schemaRegistryClient">
        ///     Confluent Schema Registry client instance.
        /// </param>
        /// <param name="schema">
        ///     Schema to use for validation, used when external
        ///     schema references are present in the schema. 
        ///     Populate the References list of the schema for
        ///     the same.
        /// </param>
        /// <param name="config">
        ///     Serializer configuration.
        /// </param>
        /// <param name="jsonSchemaGeneratorSettings">
        ///     JSON schema generator settings.
        /// </param>
        public ReferenceSchemaBasedJsonSerializer(ISchemaRegistryClient schemaRegistryClient, Schema schema, JsonSerializerConfig config = null, JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null)
            : base(schemaRegistryClient, schema, config, jsonSchemaGeneratorSettings){}
    }
}