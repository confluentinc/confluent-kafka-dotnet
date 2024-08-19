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

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NJsonSchema;
using NJsonSchema.Generation;
using Newtonsoft.Json.Linq;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     JSON Schema Resolver.
    /// </summary>
    /// <remarks>
    ///     JsonSchemaResolver provides GetResolvedSchema() function that
    ///     can be used to get the NJsonSchema.JsonSchema object corresponding to a
    ///     resolved parent schema with a list of reference schemas. Assuming that the
    ///     references have been registered in the schema registry already.
    /// </remarks>
    public class JsonSchemaResolver
    {
        private JsonSchema resolvedJsonSchema;
        private Schema root;
        private ISchemaRegistryClient schemaRegistryClient;
        private JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings;
        private Dictionary<string, Schema> dictSchemaNameToSchema = new Dictionary<string, Schema>();
        private Dictionary<string, JsonSchema> dictSchemaNameToJsonSchema = new Dictionary<string, JsonSchema>();

        /// <summary>
        ///     Initialize a new instance of the JsonSchemaResolver class.
        /// </summary>
        /// <param name="schemaRegistryClient">
        ///     Confluent Schema Registry client instance that would be used to fetch
        ///     the reference schemas.
        /// </param>
        /// <param name="schema">
        ///     Schema to use for validation, used when external
        ///     schema references are present in the schema. 
        ///     Populate the References list of the schema for
        ///     the same.
        /// </param>
        /// <param name="jsonSchemaGeneratorSettings">
        ///     Schema generator setting to use.
        /// </param>
        public JsonSchemaResolver(ISchemaRegistryClient schemaRegistryClient, Schema schema, JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null)
        {
            this.schemaRegistryClient = schemaRegistryClient;
            this.root = schema;
            this.jsonSchemaGeneratorSettings = jsonSchemaGeneratorSettings;
        }

        /// <summary>
        ///     Get the resolved JsonSchema instance for the Schema provided to
        ///     the constructor.
        /// </summary>
        public async Task<JsonSchema> GetResolvedSchema()
        {
            if (resolvedJsonSchema == null)
            {
                await CreateSchemaDictUtil(root);
                resolvedJsonSchema = await GetSchemaUtil(root);
            }
            return resolvedJsonSchema;
        }

        private async Task CreateSchemaDictUtil(Schema root)
        {
            string rootStr = root.SchemaString;
            JObject schema = JObject.Parse(rootStr);
            string schemaId = (string)schema["$id"];
            if (schemaId != null && !dictSchemaNameToSchema.ContainsKey(schemaId))
                this.dictSchemaNameToSchema.Add(schemaId, root);

            if (root.References != null)
            {
                foreach (var reference in root.References)
                {
                    Schema refSchemaRes = await schemaRegistryClient.GetRegisteredSchemaAsync(reference.Subject, reference.Version);
                    await CreateSchemaDictUtil(refSchemaRes);
                }
            }
        }

        private async Task<JsonSchema> GetSchemaUtil(Schema root)
        {
            List<SchemaReference> refers = root.References ?? new List<SchemaReference>();
            foreach (var x in refers)
            {
                if (!dictSchemaNameToJsonSchema.ContainsKey(x.Name))
                {
                    var jsonSchema = await GetSchemaUtil(dictSchemaNameToSchema[x.Name]);
                    dictSchemaNameToJsonSchema.Add(x.Name, jsonSchema);
                }
            }

            Func<JsonSchema, JsonReferenceResolver> factory;
            factory = rootObject =>
            {
                NJsonSchema.Generation.JsonSchemaResolver schemaResolver =
                    new NJsonSchema.Generation.JsonSchemaResolver(rootObject, this.jsonSchemaGeneratorSettings ??
                        new JsonSchemaGeneratorSettings());

                JsonReferenceResolver referenceResolver =
                    new JsonReferenceResolver(schemaResolver);
                foreach (var reference in refers)
                {
                    JsonSchema jschema =
                        dictSchemaNameToJsonSchema[reference.Name];
                    referenceResolver.AddDocumentReference(reference.Name, jschema);
                }
                return referenceResolver;
            };

            string rootStr = root.SchemaString;
            JObject schema = JObject.Parse(rootStr);
            string schemaId = (string)schema["$id"];
            return await JsonSchema.FromJsonAsync(rootStr, schemaId, factory);
        }
    }
}
