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
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using NJsonSchema;
using NJsonSchema.References;
using Newtonsoft.Json.Linq;
#if NET8_0_OR_GREATER
using NewtonsoftJsonSchemaGeneratorSettings = NJsonSchema.NewtonsoftJson.Generation.NewtonsoftJsonSchemaGeneratorSettings;
#else
using NewtonsoftJsonSchemaGeneratorSettings = NJsonSchema.Generation.JsonSchemaGeneratorSettings;
#endif

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
        private NewtonsoftJsonSchemaGeneratorSettings jsonSchemaGeneratorSettings;
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
        public JsonSchemaResolver(ISchemaRegistryClient schemaRegistryClient, Schema schema,
            NewtonsoftJsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null){
            this.schemaRegistryClient = schemaRegistryClient;
            this.root = schema;
            this.jsonSchemaGeneratorSettings = jsonSchemaGeneratorSettings;
        }
        
        /// <summary>
        ///     Get the resolved JsonSchema instance for the Schema provided to
        ///     the constructor.
        /// </summary>
        public async Task<JsonSchema> GetResolvedSchema(){
            if (resolvedJsonSchema == null)
            {
                await CreateSchemaDictUtil(root);
                resolvedJsonSchema = await GetSchemaUtil(root);
            }
            return resolvedJsonSchema;
        }
        
        private async Task CreateSchemaDictUtil(Schema root, string referenceName = null)
        {
            if (referenceName != null && !dictSchemaNameToSchema.ContainsKey(referenceName))
                this.dictSchemaNameToSchema.Add(referenceName, root);

            if (root.References != null)
            {
                foreach (var reference in root.References)
                {
                    Schema refSchemaRes = await schemaRegistryClient.GetRegisteredSchemaAsync(reference.Subject, reference.Version, false);
                    await CreateSchemaDictUtil(refSchemaRes, reference.Name);
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
                        new NewtonsoftJsonSchemaGeneratorSettings());

                return new CustomJsonReferenceResolver(schemaResolver, rootObject, dictSchemaNameToJsonSchema);
            };

            string rootStr = root.SchemaString;
            JObject schema = JObject.Parse(rootStr);
            string schemaId = (string)schema["$id"] ?? "";
            return await JsonSchema.FromJsonAsync(rootStr, schemaId, factory);
        }

        private class CustomJsonReferenceResolver : JsonReferenceResolver
        {
            private JsonSchema rootObject;
            private Dictionary<string, JsonSchema> refs;

            public CustomJsonReferenceResolver(JsonSchemaAppender schemaAppender,
                JsonSchema rootObject, Dictionary<string, JsonSchema> refs)
                : base( schemaAppender)
            {
                this.rootObject = rootObject;
                this.refs = refs;
            }

            public override string ResolveFilePath(string documentPath, string jsonPath)
            {
                // override the default behavior to not prepend the documentPath
                var arr = Regex.Split(jsonPath, @"(?=#)");
                return arr[0];
            }

            public override async Task<IJsonReference> ResolveFileReferenceAsync(string filePath, CancellationToken cancellationToken = default)
            {
                JsonSchema schema;
                if (refs.TryGetValue(filePath, out schema))
                {
                    return schema;
                }

                // remove the documentPath and look for the reference
                var fileName = Path.GetFileName(filePath);
                if (refs.TryGetValue(fileName, out schema))
                {
                    return schema;
                }

                return await base.ResolveFileReferenceAsync(filePath, cancellationToken);
            }

            public override async Task<IJsonReference> ResolveUrlReferenceAsync(string url, CancellationToken cancellationToken = default)
            {
                JsonSchema schema;
                if (refs.TryGetValue(url, out schema))
                {
                    return schema;
                }

                var documentPathProvider = rootObject as IDocumentPathProvider;
                var documentPath = documentPathProvider?.DocumentPath;
                if (documentPath != null)
                {
                    var documentUri = new Uri(documentPath);
                    var uri = new Uri(url);
                    var relativeUrl = documentUri.MakeRelativeUri(uri);

                    // remove the documentPath and look for the reference
                    if (refs.TryGetValue(relativeUrl.ToString(), out schema))
                    {
                        return schema;
                    }
                }

                return await base.ResolveUrlReferenceAsync(url, cancellationToken);
            }
        }
    }
}
