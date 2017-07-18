using System.Collections.Generic;

namespace  Confluent.Kafka.SchemaRegistry.Rest
{
    public static class Versions
    {
        public const string SCHEMA_REGISTRY_V1_JSON = "application/vnd.schemaregistry.v1+json";
        // Default weight = 1
        public const string SCHEMA_REGISTRY_V1_JSON_WEIGHTED = SCHEMA_REGISTRY_V1_JSON;
        // These are defaults that track the most recent API version. These should always be specified
        // anywhere the latest version is produced/consumed.
        public const string SCHEMA_REGISTRY_MOST_SPECIFIC_DEFAULT = SCHEMA_REGISTRY_V1_JSON;
        public const string SCHEMA_REGISTRY_DEFAULT_JSON = "application/vnd.schemaregistry+json";
        public const string SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED =
            SCHEMA_REGISTRY_DEFAULT_JSON +
            "; qs=0.9";
        public const string JSON = "application/json";
        public const string JSON_WEIGHTED = JSON + "; qs=0.5";

        public static readonly IReadOnlyList<string> PREFERRED_RESPONSE_TYPES = new List<string> { SCHEMA_REGISTRY_V1_JSON, SCHEMA_REGISTRY_DEFAULT_JSON, JSON };

        // This type is completely generic and carries no actual information about the type of data, but
        // it is the default for request entities if no content type is specified. Well behaving users
        // of the API will always specify the content type, but ad hoc use may omit it. We treat this as
        // JSON since that's all we currently support.
        public const string GENERIC_REQUEST = "application/octet-stream";
    }
}
