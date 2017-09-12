using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka.SchemaRegistry.Rest.Entities;
using Confluent.Kafka.SchemaRegistry.Rest.Entities.Requests;

namespace  Confluent.Kafka.SchemaRegistry.Rest
{
    public interface ISchemaRegistryRestService
    {
        Task<Schema> CheckSchemaAsync(string subject, string schema);
        Task<Config> GetCompatibilityAsync(string subject);
        Task<Config> GetGlobalCompatibilityAsync();
        Task<Schema> GetLatestSchemaAsync(string subject);
        Task<SchemaString> GetSchemaAsync(int id);
        Task<Schema> GetSchemaAsync(string subject, int version);
        Task<List<string>> GetSubjectsAsync();
        Task<List<string>> GetSubjectVersions(string subject);
        Task<SchemaId> PostSchemaAsync(string subject, string schema);
        Task<Config> PutCompatibilityAsync(string subject, Config.Compatbility compatibility);
        Task<Config> PutGlobalCompatibilityAsync(Config.Compatbility compatibility);
        Task<CompatibilityCheckResponse> TestCompatibilityAsync(string subject, int versionId, string avroSchema);
        Task<CompatibilityCheckResponse> TestLatestCompatibilityAsync(string subject, string avroSchema);
    }
}
