using NJsonSchema.Generation;

namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     Non Generic Json Serializer.
    /// </summary>
    /// <remarks>
    ///     Does the same tasks as JsonSerializer class, this is to be used when
    ///     the user wants to pass a Schema object to the serializer instead of a 
    ///     class.
    ///     Note: It requires ProducerBuilder(string,object) for creating the producer
    ///     as it is equivalent to JsonSerializer(object).
    /// </remarks>
    public class NonGenericJsonSerializer : JsonSerializer<object>
    {
        /// <summary>
        ///     Initialize a new instance of the NonGenericJsonSerializer class.
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
        public NonGenericJsonSerializer(ISchemaRegistryClient schemaRegistryClient, Schema schema, JsonSerializerConfig config = null, JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null)
            : base(schemaRegistryClient, schema, config, jsonSchemaGeneratorSettings){}
    }
}