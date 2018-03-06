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
using System.Collections.Concurrent;

namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     A caching Schema Registry client.
    /// </summary>
    public class CachedSchemaRegistryClient : ISchemaRegistryClient, IDisposable
    {
        private const string SchemaRegistryUrlPropertyName = "schema.registry.url";
        private const string SchemaRegistryConnectionTimeoutMsPropertyName = "schema.registry.connection.timeout.ms";
        private const string SchemaRegistryMaxCachedSchemasPropertyName = "schema.registry.max.cached.schemas";

        private IRestService restService;
        private readonly int identityMapCapacity;
        private readonly ConcurrentDictionary<int, string> schemaById = new ConcurrentDictionary<int, string>();
        private readonly ConcurrentDictionary<string /*subject*/, ConcurrentDictionary<string, int>> idBySchemaBySubject = new ConcurrentDictionary<string, ConcurrentDictionary<string, int>>();
        private readonly ConcurrentDictionary<string /*subject*/, ConcurrentDictionary<int, string>> schemaByVersionBySubject = new ConcurrentDictionary<string, ConcurrentDictionary<int, string>>();

        /// <summary>
        ///     The default timeout value for Schema Registry REST API calls.
        /// </summary>
        public const int DefaultTimeout = 30000;

        /// <summary>
        ///     The default maximum capacity of the local schema cache.
        /// </summary>
        public const int DefaultMaxCapacity = 1000;

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

            var schemaRegistryUrisMaybe = config.Where(prop => prop.Key.ToLower() == SchemaRegistryUrlPropertyName).FirstOrDefault();
            if (schemaRegistryUrisMaybe.Value == null)
            {
                throw new ArgumentException("schema.registry.url configuration property must be specified.");
            }
            var schemaRegistryUris = (string)schemaRegistryUrisMaybe.Value;

            var timeoutMsMaybe = config.Where(prop => prop.Key.ToLower() == SchemaRegistryConnectionTimeoutMsPropertyName).FirstOrDefault();
            var timeoutMs = timeoutMsMaybe.Value == null ? DefaultTimeout : (int)timeoutMsMaybe.Value;

            var identityMapCapacityMaybe = config.Where(prop => prop.Key.ToLower() == SchemaRegistryMaxCachedSchemasPropertyName).FirstOrDefault();
            this.identityMapCapacity = identityMapCapacityMaybe.Value == null ? DefaultMaxCapacity : (int)identityMapCapacityMaybe.Value;

            foreach (var property in config)
            {
                if (!property.Key.StartsWith("schema.registry."))
                {
                    continue;
                }

                if (property.Key != SchemaRegistryUrlPropertyName &&
                    property.Key != SchemaRegistryConnectionTimeoutMsPropertyName &&
                    property.Key != SchemaRegistryMaxCachedSchemasPropertyName)
                {
                    throw new ArgumentException($"CachedSchemaRegistryClient: unexpected configuration parameter {property.Key}");
                }
            }

            this.restService = new RestService(schemaRegistryUris, timeoutMs);
        }

        /// <remarks>
        ///     This is to make sure memory doesn't explode in the case of incorrect usage.
        /// 
        ///     It's behavior is pretty extreme - remove everything and start again if the 
        ///     cache gets full. However, in practical situations this is not expected.
        /// 
        ///     TODO: Implement an LRU Cache here or something instead.
        /// </remarks>
        private bool CleanCacheIfFull()
        {
            if (schemaById.Count >= identityMapCapacity)
            {
                // TODO: maybe log something somehow if this happens. Maybe throwing an exception (fail fast) is better.
                this.schemaById.Clear();
                this.idBySchemaBySubject.Clear();
                this.schemaByVersionBySubject.Clear();
                return true;
            }

            return false;
        }

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetSchemaIdAsync"]/*' />
        public async Task<int> GetSchemaIdAsync(string subject, string schema)
        {
            if (!this.idBySchemaBySubject.TryGetValue(subject, out ConcurrentDictionary<string, int> idBySchema))
            {
                idBySchema = new ConcurrentDictionary<string, int>();
                if (!this.idBySchemaBySubject.TryAdd(subject, idBySchema))
                {
                    //Race conditions.
                    this.idBySchemaBySubject.TryGetValue(subject, out var idBySchema2);
                    idBySchema = idBySchema2 ?? idBySchema;
                }
            }

            // TODO: This could be optimized in the usual case where idBySchema only
            // contains very few elements and the schema string passed in is always
            // the same instance.

            if (!idBySchema.TryGetValue(schema, out int schemaId))
            {
                CleanCacheIfFull();

                schemaId = (await restService.CheckSchemaAsync(subject, schema, true).ConfigureAwait(false)).Id;
                idBySchema[schema] = schemaId;
                schemaById[schemaId] = schema;
            }

            return schemaId;
        }

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_RegisterSchemaAsync"]/*' />
        public async Task<int> RegisterSchemaAsync(string subject, string schema)
        {
            if (!this.idBySchemaBySubject.TryGetValue(subject, out ConcurrentDictionary<string, int> idBySchema))
            {
                idBySchema = new ConcurrentDictionary<string, int>();
                this.idBySchemaBySubject[subject] = idBySchema;
            }

            // TODO: This could be optimized in the usual case where idBySchema only
            // contains very few elements and the schema string passed in is always
            // the same instance.

            if (!idBySchema.TryGetValue(schema, out int schemaId))
            {
                CleanCacheIfFull();

                schemaId = await restService.RegisterSchemaAsync(subject, schema).ConfigureAwait(false);

                idBySchema[schema] = schemaId;
                schemaById[schemaId] = schema;
            }

            return schemaId;
        }

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetSchemaAsync"]/*' />
        public async Task<string> GetSchemaAsync(int id)
        {
            if (!this.schemaById.TryGetValue(id, out var schema))
            {
                CleanCacheIfFull();

                schema = await restService.GetSchemaAsync(id).ConfigureAwait(false);
                schemaById[id] = schema;
            }

            return schema;
        }

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetSchemaAsyncSubjectVersion"]/*' />
        public async Task<string> GetSchemaAsync(string subject, int version)
        {
            CleanCacheIfFull();

            if (!schemaByVersionBySubject.TryGetValue(subject, out ConcurrentDictionary<int, string> schemaByVersion))
            {
                schemaByVersion = new ConcurrentDictionary<int, string>();
                schemaByVersionBySubject[subject] = schemaByVersion;
            }

            if (!schemaByVersion.TryGetValue(version, out string schemaString))
            {
                var schema = await restService.GetSchemaAsync(subject, version);
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

        /// <summary>
        ///     Releases unmanaged resources owned by this CachedSchemaRegistryClient instance.
        /// </summary>
        public void Dispose()
            => restService.Dispose();
    }
}
