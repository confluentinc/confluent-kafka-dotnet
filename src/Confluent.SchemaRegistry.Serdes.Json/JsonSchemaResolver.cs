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

        private void CreateSchemaDictUtil(Schema root)
        {
            string root_str = root.SchemaString;
            JObject schema = JObject.Parse(root_str);
            string schemaId = (string)schema["$id"];
            if (!dictSchemaNameToSchema.ContainsKey(schemaId))
                this.dictSchemaNameToSchema.Add(schemaId, root);

            foreach (var reference in root.References)
            {
                Schema ref_schema_res = this.schemaRegistryClient.GetRegisteredSchemaAsync(reference.Subject, reference.Version).Result;
                CreateSchemaDictUtil(ref_schema_res);
            }
        }

        private JsonSchema GetSchemaUtil(Schema root)
        {
            List<SchemaReference> refers = root.References;
            foreach (var x in refers)
            {
                if (!dictSchemaNameToJsonSchema.ContainsKey(x.Name))
                    dictSchemaNameToJsonSchema.Add(
                        x.Name, GetSchemaUtil(dictSchemaNameToSchema[x.Name]));
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

            string root_str = root.SchemaString;
            JObject schema = JObject.Parse(root_str);
            string schemaId = (string)schema["$id"];
            JsonSchema root_schema = JsonSchema.FromJsonAsync(root_str, schemaId, factory).Result;
            return root_schema;
        }

        /// <summary>
        ///     Get the resolved JsonSchema instance for the Schema provided to
        ///     the constructor.
        /// </summary>
        public JsonSchema GetResolvedSchema(){
            return this.resolvedJsonSchema;
        }

        /// <summary>
        ///     Initialize a new instance of the JsonSerDesSchemaUtils class.
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
        public JsonSchemaResolver(ISchemaRegistryClient schemaRegistryClient, Schema schema, JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null){
            this.schemaRegistryClient = schemaRegistryClient;
            this.root = schema;
            this.jsonSchemaGeneratorSettings = jsonSchemaGeneratorSettings;
            CreateSchemaDictUtil(root);
            this.resolvedJsonSchema = GetSchemaUtil(root);
        }
    }
}
