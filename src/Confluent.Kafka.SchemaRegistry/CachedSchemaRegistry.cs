// Copyright 2016-2017 Confluent Inc.
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

using Confluent.Kafka.SchemaRegistry.Rest;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;


namespace Confluent.Kafka.SchemaRegistry
{
    /// <summary>
    ///     A Schema Registry client that caches request responses.
    /// </summary>
    public class CachedSchemaRegistryClient : ISchemaRegistryClient
    {
        private ISchemaRegistyRestService restService;
        private readonly int identityMapCapacity;
        private readonly Dictionary<int, string> schemaById = new Dictionary<int, string>();
        private readonly Dictionary<string /*subject*/, Dictionary<string, int>> idBySchemaBySubject = new Dictionary<string, Dictionary<string, int>>();
        private readonly Dictionary<string /*subject*/, Dictionary<int, string>> schemaByVersionBySubject = new Dictionary<string, Dictionary<int, string>>();
        
        /// <summary>
        ///     Initialize a new instance of the CachedSchemaRegistryClient
        /// </summary>
        /// <param name="schemaRegistryConfig">
        ///     
        /// </param>
        /// <param name="timeoutMs">
        ///     
        /// </param>
        /// <param name="identityMapCapacity">
        ///     maximum stored schemas by in the cache, cache is wiped when this limit is hit
        /// </param>
        public CachedSchemaRegistryClient(
            string schemaRegistryUris, 
            int timeoutMs = SchemaRegistryRestService.DefaultTimeout,
            int identityMapCapacity = 1024)
        {
            this.identityMapCapacity = identityMapCapacity;
            this.restService = new SchemaRegistryRestService(schemaRegistryUris, timeoutMs);
        }

        private bool CleanCacheIfNeeded()
        {
            // call before inserting a new element

            // just to make sure we don't explose memory due to wrong usage
            // don't check _idBySchemaBySubject, it's directly related with both others
            if(schemaById.Count + schemaByVersionBySubject.Sum(x=>x.Value.Count) >= identityMapCapacity)
            {
                // TODO log
                schemaById.Clear();
                idBySchemaBySubject.Clear();
                schemaByVersionBySubject.Clear();
                return true;
            }
            return false;
        }

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_RegisterAsync"]/*' />
        public async Task<int> RegisterAsync(string subject, string schema)
        {
            // Can't check schemaById, we need to register under a specific subject
            Dictionary<string, int> idBySchema;
            if (!idBySchemaBySubject.TryGetValue(subject, out idBySchema))
            {
                idBySchema = new Dictionary<string, int>();
                idBySchemaBySubject[subject] = idBySchema;
            }

            int schemaId;
            if (!idBySchema.TryGetValue(schema, out schemaId))
            {
                var register = await restService.PostSchemaAsync(subject, schema).ConfigureAwait(false);
                if (CleanCacheIfNeeded())
                {
                    //must register again
                    idBySchemaBySubject[subject] = idBySchema;
                }
                schemaId = register.Id;
                idBySchema[schema] = schemaId;
                schemaById[schemaId] = schema;
            }
            return schemaId;
        }

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetSchemaAsync"]/*' />
        public async Task<string> GetSchemaAsync(int id)
        {
            string schema;
            if (!schemaById.TryGetValue(id, out schema))
            {
                var getSchema = await restService.GetSchemaAsync(id).ConfigureAwait(false);
                CleanCacheIfNeeded();
                schema = getSchema.Schema;
                schemaById[id] = schema;
            }
            return schema;
        }

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetSchemaAsync_II"]/*' />
        public async Task<string> GetSchemaAsync(string subject, int version)
        {
            if (!schemaByVersionBySubject.TryGetValue(subject, out Dictionary<int, string> schemaByVersion))
            {
                schemaByVersion = new Dictionary<int, string>();
                schemaByVersionBySubject[subject] = schemaByVersion;
            }

            if (!schemaByVersion.TryGetValue(version, out string schema))
            {
                var getSchema = await restService.GetSchemaAsync(subject, version).ConfigureAwait(false);
                if (CleanCacheIfNeeded())
                {
                    // repopulate this one
                    schemaByVersionBySubject[subject] = schemaByVersion;
                }
                schema = getSchema.SchemaString;
                schemaByVersion[version] = schema;
                schemaById[getSchema.Id] = schema;
            }

            return schema;
        }

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetLatestSchemaAsync"]/*' />
        public async Task<Schema> GetLatestSchemaAsync(string subject)
        {
            var getLatestSchema = await restService.GetLatestSchemaAsync(subject).ConfigureAwait(false);
            return getLatestSchema;
        }

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetAllSubjectsAsync"]/*' />
        public Task<List<string>> GetAllSubjectsAsync()
        {
            return restService.GetSubjectsAsync();
        }

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_IsCompatibleAsync"]/*' />
        public async Task<bool> IsCompatibleAsync(string subject, string avroSchema)
        {
            var getLatestCompatibility = await restService.TestLatestCompatibilityAsync(subject, avroSchema).ConfigureAwait(false);
            return getLatestCompatibility.IsCompatible;
        }

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_ConstructRegistrySubject"]/*' />
        public string ConstructRegistrySubject(string topic, SubjectType keyOrValue)
            => $"{topic}-{(keyOrValue == SubjectType.Key ? "key" : "value")}";
    }
}
