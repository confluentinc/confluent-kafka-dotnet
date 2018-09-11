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
using System.Threading;


namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     A caching Schema Registry client.
    /// </summary>
    public class CachedSchemaRegistryClient : ISchemaRegistryClient, IDisposable
    {
        // note: these property names are also specified in Confluent.Kafka.ConfigPropertyNames,
        // which acts as a central location where all config properties are documented. However,
        // Confluent.SchemaRegistry doesn't depend on Confluent.Kafka, so these are redefined here.
        // If these change, they should be kept in-sync in the two places.
        private const string SchemaRegistryUrlPropertyName = "schema.registry.url";
        private const string SchemaRegistryConnectionTimeoutMsPropertyName = "schema.registry.request.timeout.ms";
        private const string SchemaRegistryMaxCachedSchemasPropertyName = "schema.registry.max.cached.schemas";

        private IRestService restService;
        private readonly int identityMapCapacity;
        private readonly Dictionary<int, string> schemaById = new Dictionary<int, string>();
        private readonly Dictionary<string /*subject*/, Dictionary<string, int>> idBySchemaBySubject = new Dictionary<string, Dictionary<string, int>>();
        private readonly Dictionary<string /*subject*/, Dictionary<int, string>> schemaByVersionBySubject = new Dictionary<string, Dictionary<int, string>>();
        private readonly object cacheLock = new object();

        /// <summary>
        ///     The default timeout value for Schema Registry REST API calls.
        /// </summary>
        public const int DefaultTimeout = 30000;

        /// <summary>
        ///     The default maximum capacity of the local schema cache.
        /// </summary>
        public const int DefaultMaxCachedSchemas = 1000;


        /// <summary>
        ///     Refer to <see cref="Confluent.SchemaRegistry.ISchemaRegistryClient.MaxCachedSchemas" />
        /// </summary>
        public int MaxCachedSchemas
            => identityMapCapacity;

        /// <summary>
        ///     Initialize a new instance of the SchemaRegistryClient class.
        /// </summary>
        /// <param name="config">
        ///     Configuration properties.
        /// </param>
        public CachedSchemaRegistryClient(SchemaRegistryConfig config)
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
            var timeoutMs = timeoutMsMaybe.Value == null ? DefaultTimeout : int.Parse(timeoutMsMaybe.Value);

            var identityMapCapacityMaybe = config.Where(prop => prop.Key.ToLower() == SchemaRegistryMaxCachedSchemasPropertyName).FirstOrDefault();
            this.identityMapCapacity = identityMapCapacityMaybe.Value == null ? DefaultMaxCachedSchemas : int.Parse(identityMapCapacityMaybe.Value);

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


        /// <summary>
        ///     Refer to <see cref="Confluent.SchemaRegistry.ISchemaRegistryClient.GetSchemaIdAsync(string, string)" />
        /// </summary>
        public Task<int> GetSchemaIdAsync(string subject, string schema)
        {
            lock (cacheLock)
            { 
                if (!this.idBySchemaBySubject.TryGetValue(subject, out Dictionary<string, int> idBySchema))
                {
                    idBySchema = new Dictionary<string, int>();
                    this.idBySchemaBySubject.Add(subject, idBySchema);
                }

                // TODO: This could be optimized in the usual case where idBySchema only
                // contains very few elements and the schema string passed in is always
                // the same instance.

                if (!idBySchema.TryGetValue(schema, out int schemaId))
                {
                    CleanCacheIfFull();

                    schemaId = restService.CheckSchemaAsync(subject, schema, true).Result.Id;
                    idBySchema[schema] = schemaId;
                    schemaById[schemaId] = schema;
                }

                return Task.FromResult(schemaId);
            }
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.SchemaRegistry.ISchemaRegistryClient.RegisterSchemaAsync(string, string)" />
        /// </summary>
        public Task<int> RegisterSchemaAsync(string subject, string schema)
        {
            lock (cacheLock)
            { 
                if (!this.idBySchemaBySubject.TryGetValue(subject, out Dictionary<string, int> idBySchema))
                {
                    idBySchema = new Dictionary<string, int>();
                    this.idBySchemaBySubject[subject] = idBySchema;
                }

                // TODO: This could be optimized in the usual case where idBySchema only
                // contains very few elements and the schema string passed in is always
                // the same instance.

                if (!idBySchema.TryGetValue(schema, out int schemaId))
                {
                    CleanCacheIfFull();

                    schemaId = restService.RegisterSchemaAsync(subject, schema).Result;
                    idBySchema[schema] = schemaId;
                    schemaById[schemaId] = schema;
                }

                return Task.FromResult(schemaId);
            }
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.SchemaRegistry.ISchemaRegistryClient.GetSchemaAsync(int)" />
        /// </summary>
        public Task<string> GetSchemaAsync(int id)
        {
            lock (cacheLock)
            { 
                if (!this.schemaById.TryGetValue(id, out string schema))
                {
                    CleanCacheIfFull();

                    schema = restService.GetSchemaAsync(id).Result;
                    schemaById[id] = schema;
                }

                return Task.FromResult(schema);
            }
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.SchemaRegistry.ISchemaRegistryClient.GetSchemaAsync(string, int)" />
        /// </summary>
        public Task<string> GetSchemaAsync(string subject, int version)
        {
            lock (cacheLock)
            { 
                CleanCacheIfFull();

                if (!schemaByVersionBySubject.TryGetValue(subject, out Dictionary<int, string> schemaByVersion))
                {
                    schemaByVersion = new Dictionary<int, string>();
                    schemaByVersionBySubject[subject] = schemaByVersion;
                }

                if (!schemaByVersion.TryGetValue(version, out string schemaString))
                {
                    var schema = restService.GetSchemaAsync(subject, version).Result;
                    schemaString = schema.SchemaString;
                    schemaByVersion[version] = schemaString;
                    schemaById[schema.Id] = schemaString;
                }

                return Task.FromResult(schemaString);
            }
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.SchemaRegistry.ISchemaRegistryClient.GetLatestSchemaAsync(string)" />
        /// </summary>
        public async Task<Schema> GetLatestSchemaAsync(string subject)
            => await restService.GetLatestSchemaAsync(subject).ConfigureAwait(continueOnCapturedContext: false);



        /// <summary>
        ///     Refer to <see cref="Confluent.SchemaRegistry.ISchemaRegistryClient.GetAllSubjectsAsync" />
        /// </summary>
        public Task<List<string>> GetAllSubjectsAsync()
            => restService.GetSubjectsAsync();


        /// <summary>
        ///     Refer to <see cref="Confluent.SchemaRegistry.ISchemaRegistryClient.IsCompatibleAsync(string, string)" />
        /// </summary>
        public async Task<bool> IsCompatibleAsync(string subject, string schema)
            => await restService.TestLatestCompatibilityAsync(subject, schema).ConfigureAwait(continueOnCapturedContext: false);


        /// <summary>
        ///     Refer to <see cref="Confluent.SchemaRegistry.ISchemaRegistryClient.ConstructKeySubjectName(string)" />
        /// </summary>
        public string ConstructKeySubjectName(string topic)
            => $"{topic}-key";


        /// <summary>
        ///     Refer to <see cref="Confluent.SchemaRegistry.ISchemaRegistryClient.ConstructValueSubjectName(string)" />
        /// </summary>
        public string ConstructValueSubjectName(string topic)
            => $"{topic}-value";


        /// <summary>
        ///     Releases unmanaged resources owned by this CachedSchemaRegistryClient instance.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        /// <summary>
        ///     Releases the unmanaged resources used by this object
        ///     and optionally disposes the managed resources.
        /// </summary>
        /// <param name="disposing">
        ///     true to release both managed and unmanaged resources;
        ///     false to release only unmanaged resources.
        /// </param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                restService.Dispose();
            }
        }
    }
}
