using Confluent.Kafka.SchemaRegistry.Rest;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;

namespace Confluent.Kafka.SchemaRegistry
{
    /// <summary>
    /// SchemaRegistry client with cached result
    /// </summary>
    public class CachedSchemaRegistryClient : ISchemaRegistryClient
    {
        private ISchemaRegistyRestService _restService;
        private readonly int _identityMapCapacity;
        private readonly Dictionary<int, string> _schemaById = new Dictionary<int, string>();
        //idBySchema not needed, we always have to register under a subject
        //so will always have to use idBySchemaAndSubject
        private readonly Dictionary<string /*subject*/, Dictionary<string, int>> _idBySchemaBySubject = new Dictionary<string, Dictionary<string, int>>();
        private readonly Dictionary<string /*subject*/, Dictionary<int, string>> _schemaByVersionBySubject = new Dictionary<string, Dictionary<int, string>>();
        
        /// <summary>
        /// Create a cached schema registry
        /// </summary>
        /// <param name="schemaRegistryConfig"></param>
        /// <param name="identityMapCapacity">maximum stored schemas by in the cache, cache is whiped when this limit is hit</param>
        public CachedSchemaRegistryClient(string schemaRegistryUris, int timeoutMs = SchemaRegistryRestService.DefaultTimetout, int identityMapCapacity = 1024)
            : this(new SchemaRegistryRestService(schemaRegistryUris), identityMapCapacity)
        { }

        /// <summary>
        /// Create a cached schema registry
        /// </summary>
        /// <param name="restService"></param>
        /// <param name="identityMapCapacity">maximum stored schemas by in the cache, cache is whiped when this limit is hit</param>
        public CachedSchemaRegistryClient(ISchemaRegistyRestService restService, int identityMapCapacity = 1024)
        {
            _identityMapCapacity = identityMapCapacity;
            _restService = restService;
        }

        private bool CleanCacheIfNeeded()
        {
            //call before inserting a new element

            //just to make sure we don't explose memory due to wrong usage
            //don't check _idBySchemaBySubject, it's directly related with both others
            if(_schemaById.Count + _schemaByVersionBySubject.Sum(x=>x.Value.Count) >= _identityMapCapacity)
            {
                //TODO log
                _schemaById.Clear();
                _idBySchemaBySubject.Clear();
                _schemaByVersionBySubject.Clear();
                return true;
            }
            return false;
        }

        public async Task<int> RegisterAsync(string subject, string schema)
        {
            //Can't check schemaById, we need to register under a specific subject
            Dictionary<string, int> idBySchema;
            if (!_idBySchemaBySubject.TryGetValue(subject, out idBySchema))
            {
                idBySchema = new Dictionary<string, int>();
                _idBySchemaBySubject[subject] = idBySchema;
            }

            int schemaId;
            if (!idBySchema.TryGetValue(schema, out schemaId))
            {
                var register = await _restService.PostSchemaAsync(subject, schema).ConfigureAwait(false);
                if (CleanCacheIfNeeded())
                {
                    //must register again
                    _idBySchemaBySubject[subject] = idBySchema;
                }
                schemaId = register.Id;
                idBySchema[schema] = schemaId;
                _schemaById[schemaId] = schema;
            }
            return schemaId;
        }

        public async Task<string> GetSchemaAsync(int id)
        {
            string schema;
            if (!_schemaById.TryGetValue(id, out schema))
            {
                var getSchema = await _restService.GetSchemaAsync(id).ConfigureAwait(false);
                CleanCacheIfNeeded();
                schema = getSchema.Schema;
                _schemaById[id] = schema;
            }
            return schema;
        }

        public async Task<string> GetSchemaAsync(string subject, int version)
        {
            if (!_schemaByVersionBySubject.TryGetValue(subject, out Dictionary<int, string> schemaByVersion))
            {
                schemaByVersion = new Dictionary<int, string>();
                _schemaByVersionBySubject[subject] = schemaByVersion;
            }

            if (!schemaByVersion.TryGetValue(version, out string schema))
            {
                var getSchema = await _restService.GetSchemaAsync(subject, version).ConfigureAwait(false);
                if (CleanCacheIfNeeded())
                {
                    //repopulate this one
                    _schemaByVersionBySubject[subject] = schemaByVersion;
                }
                schema = getSchema.SchemaString;
                schemaByVersion[version] = schema;
                _schemaById[getSchema.Id] = schema;
            }

            return schema;
        }

        /// <summary>
        /// Get latest known subject
        /// Always call web api
        /// </summary>
        /// <param name="subject"></param>
        /// <returns></returns>
        public async Task<Schema> GetLatestSchemaAsync(string subject)
        {
            var getLatestSchema = await _restService.GetLatestSchemaAsync(subject).ConfigureAwait(false);
            return getLatestSchema;
        }

        /// <summary>
        /// Get all subjects
        /// Always call web api
        /// </summary>
        /// <returns></returns>
        public Task<List<string>> GetAllSubjectsAsync()
        {
            return _restService.GetSubjectsAsync();
        }

        public async Task<bool> IsCompatibleAsync(string subject, string avroSchema)
        {
            var getLatestCompatibility = await _restService.TestLatestCompatibilityAsync(subject, avroSchema).ConfigureAwait(false);
            return getLatestCompatibility.IsCompatible;
        }

        public string GetRegistrySubject(string topic, bool isKey)
        {
            return $"{topic}-{(isKey ? "key" : "value")}";
        }
    }
}
