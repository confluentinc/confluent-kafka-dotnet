using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;
using NJsonSchema;
using NJsonSchema.Generation;
using Newtonsoft.Json.Linq;
namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     JSON SerDes Schema Utils.
    /// </summary>
    /// <remarks>
    ///     JsonSerDesSchemaUtils provides getResolvedSchema() function that
    ///     can be used to get the NJsonSchema.JsonSchema object corresponding to a
    ///     resolved parent schema with a list of reference schemas. Assuming that the
    ///     references have been registered in the schema registry already.
    /// </remarks>
    public class JsonSerDesSchemaUtils
    {
        private JsonSchema resolvedJsonSchema;
        private Schema root;
        private ISchemaRegistryClient schemaRegistryClient;
        private Dictionary<string, Schema> dict_schema_name_to_schema = new Dictionary<string, Schema>();
        private Dictionary<string, JsonSchema> dict_schema_name_to_JsonSchema = new Dictionary<string, JsonSchema>();
        private static int curRefNo = 0;
        private static bool IsIntegerEnumSchemaUtil(JObject schema)
        {
            JToken typeToken = schema["type"];
            JToken enumToken = schema["enum"];
            if (typeToken != null && enumToken != null && typeToken.Type == JTokenType.String && typeToken.Value<string>() == "integer")
            {
                if (enumToken.Type == JTokenType.Array)
                {
                    foreach (JToken enumValue in enumToken)
                    {
                        if (enumValue.Type != JTokenType.Integer)
                        {
                            return false;
                        }
                    }
                    return true;
                }
            }
            return false;
        }
        private static Type GetTypeForSchema()
        {
            string className = "RefSchema" + curRefNo.ToString();
            curRefNo++;
            string nameSpace = "Confluent.SchemaRegistry.Serdes";
            AssemblyName assemblyName = new AssemblyName(nameSpace);
            AssemblyBuilder assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.RunAndCollect);
            ModuleBuilder moduleBuilder = assemblyBuilder.DefineDynamicModule(nameSpace);
            TypeBuilder typeBuilder = moduleBuilder.DefineType(nameSpace + "." + className, TypeAttributes.Public);
            Type dynamicType = typeBuilder.CreateTypeInfo().AsType();
            return dynamicType;
        }
        private void create_schema_dict_util(Schema root)
        {
            string root_str = root.SchemaString;
            JObject schema = JObject.Parse(root_str);
            string schemaId = (string)schema["$id"];
            if (!dict_schema_name_to_schema.ContainsKey(schemaId))
                this.dict_schema_name_to_schema.Add(schemaId, root);

            foreach (var reference in root.References)
            {
                Schema ref_schema_res = this.schemaRegistryClient.GetRegisteredSchemaAsync(reference.Subject, reference.Version).Result;
                create_schema_dict_util(ref_schema_res);
            }
        }

        private JsonSchema getSchemaUtil(Schema root)
        {
            List<SchemaReference> refers = root.References;
            foreach (var x in refers)
            {
                if (!dict_schema_name_to_JsonSchema.ContainsKey(x.Name))
                    dict_schema_name_to_JsonSchema.Add(
                        x.Name, getSchemaUtil(dict_schema_name_to_schema[x.Name]));
            }

            Func<JsonSchema, JsonReferenceResolver> factory;
            factory = x =>
            {
                JsonSchemaResolver schemaResolver =
                    new JsonSchemaResolver(x, new JsonSchemaGeneratorSettings());
                foreach (var reference in refers)
                {
                    JsonSchema jschema =
                        dict_schema_name_to_JsonSchema[reference.Name];
                    Type type = GetTypeForSchema();
                    JObject schemaObject = JObject.Parse(jschema.ToJson());
                    var isIntEnum = IsIntegerEnumSchemaUtil(schemaObject);
                    schemaResolver.AddSchema(type, isIntEnum, jschema);
                }
                JsonReferenceResolver referenceResolver =
                    new JsonReferenceResolver(schemaResolver);
                foreach (var reference in refers)
                {
                    JsonSchema jschema =
                        dict_schema_name_to_JsonSchema[reference.Name];
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
        public JsonSchema getResolvedSchema(){
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
        public JsonSerDesSchemaUtils(ISchemaRegistryClient schemaRegistryClient, Schema schema){
            this.schemaRegistryClient = schemaRegistryClient;
            this.root = schema;
            create_schema_dict_util(root);
            this.resolvedJsonSchema = getSchemaUtil(root);
        }
    }
}