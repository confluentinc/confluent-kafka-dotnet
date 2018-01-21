// Copyright 2016-2018 Confluent Inc.
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

using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using System;


namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     A Schema Registry client that caches request responses.
    /// </summary>
    public class CachedSchemaRegistryClient : ISchemaRegistryClient, IDisposable
    {
        private IRestService restService;
        private readonly int identityMapCapacity;
        private readonly Dictionary<int, string> schemaById = new Dictionary<int, string>();
        private readonly Dictionary<string /*subject*/, Dictionary<string, int>> idBySchemaBySubject = new Dictionary<string, Dictionary<string, int>>();
        private readonly Dictionary<string /*subject*/, Dictionary<int, string>> schemaByVersionBySubject = new Dictionary<string, Dictionary<int, string>>();
        
        /// <summary>
        ///     The default timeout value for Schema Registry REST API calls.
        /// </summary>
        public const int DefaultTimeout = 10000;

        /// <summary>
        ///     The default maximum capacity of the local schema cache.
        /// </summary>
        public const int DefaultMaxCapacity = 1024;

        /// <summary>
        ///     Initialize a new instance of the SchemaRegistryClient class.
        /// </summary>
        /// <param name="config">
        ///     Configuration properties.
        /// </param>
        public CachedSchemaRegistryClient(IEnumerable<KeyValuePair<string, object>> config)
        {
            if (config == null)
            {
                throw new ArgumentNullException("config properties must be specified.");
            }

            var schemaRegistryUrisMaybe = config.Where(prop => prop.Key.ToLower() == "schema.registry.url").FirstOrDefault();
            if (schemaRegistryUrisMaybe.Value == null)
            {
                throw new ArgumentException("schema.registry.url configuration property must be specified.");
            }
            var schemaRegistryUris = (string)schemaRegistryUrisMaybe.Value;

            var timeoutMsMaybe = config.Where(prop => prop.Key.ToLower() == "schema.registry.timeout.ms").FirstOrDefault();
            var timeoutMs = timeoutMsMaybe.Value == null ? DefaultTimeout : (int)timeoutMsMaybe.Value;

            var identityMapCapacityMaybe = config.Where(prop => prop.Key.ToLower() == "schema.registry.cache.capacity").FirstOrDefault();
            this.identityMapCapacity = identityMapCapacityMaybe.Value == null ? DefaultMaxCapacity : (int)identityMapCapacityMaybe.Value;

            this.restService = new RestService(schemaRegistryUris, timeoutMs);
        }

        /// <remarks>
        ///     Ensure memory doesn't explode in the case of incorrect usage.
        /// </remarks>
        private void CheckIfCacheFull()
        {
            if (
                this.schemaById.Count + 
                this.schemaByVersionBySubject.Sum(x => x.Value.Count) +
                this.idBySchemaBySubject.Sum(x => x.Value.Count)
                    >= identityMapCapacity)
            {
                throw new OutOfMemoryException("Local schema cache maximum capacity exceeded");
            }
        }

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetIdAsync"]/*' />
        public async Task<int> GetIdAsync(string subject, string schema)
        {
            CheckIfCacheFull();

            if (!this.idBySchemaBySubject.TryGetValue(subject, out Dictionary<string, int> idBySchema))
            {
                idBySchema = new Dictionary<string, int>();
                this.idBySchemaBySubject[subject] = idBySchema;
            }

            if (!idBySchema.TryGetValue(schema, out int schemaId))
            {
                schemaId = (await restService.CheckSchemaAsync(subject, schema, true)).Id;
                idBySchema[schema] = schemaId;
                schemaById[schemaId] = schema;
            }

            return schemaId;
        }

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_RegisterAsync"]/*' />
        public async Task<int> RegisterAsync(string subject, string schema)
        {
            CheckIfCacheFull();
            
            if (!this.idBySchemaBySubject.TryGetValue(subject, out Dictionary<string, int> idBySchema))
            {
                idBySchema = new Dictionary<string, int>();
                this.idBySchemaBySubject[subject] = idBySchema;
            }

            if (!idBySchema.TryGetValue(schema, out int schemaId))
            {
                schemaId = await restService.RegisterSchemaAsync(subject, schema).ConfigureAwait(false);
                idBySchema[schema] = schemaId;
                schemaById[schemaId] = schema;
            }

            return schemaId;
        }

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetSchemaAsync"]/*' />
        public async Task<string> GetSchemaAsync(int id)
        {
            CheckIfCacheFull();

            if (!this.schemaById.TryGetValue(id, out string schema))
            {
                schema = await restService.GetSchemaAsync(id).ConfigureAwait(false);
                schemaById[id] = schema;
            }

            return schema;
        }

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetSchemaAsyncSubjectVersion"]/*' />
        public async Task<string> GetSchemaAsync(string subject, int version)
        {
            CheckIfCacheFull();

            if (!schemaByVersionBySubject.TryGetValue(subject, out Dictionary<int, string> schemaByVersion))
            {
                schemaByVersion = new Dictionary<int, string>();
                schemaByVersionBySubject[subject] = schemaByVersion;
            }

            if (!schemaByVersion.TryGetValue(version, out string schemaString))
            {
                var schema = await restService.GetSchemaAsync(subject, version).ConfigureAwait(false);
                schemaString = schema.SchemaString;
                schemaByVersion[version] = schemaString;
                schemaById[schema.Id] = schemaString;
            }

            return schemaString;
        }

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetLatestSchemaAsync"]/*' />
        public async Task<Schema> GetLatestSchemaAsync(string subject)
            => await restService.GetLatestSchemaAsync(subject).ConfigureAwait(false);

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetAllSubjectsAsync"]/*' />
        public Task<List<string>> GetAllSubjectsAsync()
            => restService.GetSubjectsAsync();

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_IsCompatibleAsync"]/*' />
        public async Task<bool> IsCompatibleAsync(string subject, string schema)
            => await restService.TestLatestCompatibilityAsync(subject, schema).ConfigureAwait(false);

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_ConstructKeySubjectName"]/*' />
        public string ConstructKeySubjectName(string topic)
            => $"{topic}-key";

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_ConstructValueSubjectName"]/*' />
        public string ConstructValueSubjectName(string topic)
            => $"{topic}-value";

        public void Dispose()
            => restService.Dispose();
    }
}
