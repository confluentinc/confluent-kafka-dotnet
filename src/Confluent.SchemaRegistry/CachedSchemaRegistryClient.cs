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
        private IRestService restService;
        private readonly int identityMapCapacity;
        private readonly Dictionary<int, string> schemaById = new Dictionary<int, string>();
        private readonly Dictionary<string /*subject*/, Dictionary<string, int>> idBySchemaBySubject = new Dictionary<string, Dictionary<string, int>>();
        private readonly Dictionary<string /*subject*/, Dictionary<int, string>> schemaByVersionBySubject = new Dictionary<string, Dictionary<int, string>>();
        private readonly SemaphoreSlim cacheMutex = new SemaphoreSlim(1);

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
        public CachedSchemaRegistryClient(IEnumerable<KeyValuePair<string, string>> config)
        {
            if (config == null)
            {
                throw new ArgumentNullException("config properties must be specified.");
            }

            var schemaRegistryUrisMaybe = config.FirstOrDefault(prop => prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryUrl);
            if (schemaRegistryUrisMaybe.Value == null) { throw new ArgumentException($"{SchemaRegistryConfig.PropertyNames.SchemaRegistryUrl} configuration property must be specified."); }
            var schemaRegistryUris = (string)schemaRegistryUrisMaybe.Value;

            var timeoutMsMaybe = config.FirstOrDefault(prop => prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryRequestTimeoutMs);
            int timeoutMs;
            try { timeoutMs = timeoutMsMaybe.Value == null ? DefaultTimeout : Convert.ToInt32(timeoutMsMaybe.Value); }
            catch (FormatException) { throw new ArgumentException($"Configured value for {SchemaRegistryConfig.PropertyNames.SchemaRegistryRequestTimeoutMs} must be an integer."); }

            var identityMapCapacityMaybe = config.FirstOrDefault(prop => prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryMaxCachedSchemas);
            try { this.identityMapCapacity = identityMapCapacityMaybe.Value == null ? DefaultMaxCachedSchemas : Convert.ToInt32(identityMapCapacityMaybe.Value); }
            catch (FormatException) { throw new ArgumentException($"Configured value for {SchemaRegistryConfig.PropertyNames.SchemaRegistryMaxCachedSchemas} must be an integer."); }

            var basicAuthSource = Convert.ToString(config.FirstOrDefault(prop => prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthCredentialsSource).Value) ?? "";
            var basicAuthInfo = Convert.ToString(config.FirstOrDefault(prop => prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthUserInfo).Value) ?? "";

            string username = null;
            string password = null;

            if (basicAuthSource == "USER_INFO" || basicAuthSource == "")
            {
                if (basicAuthInfo != "")
                {
                    var userPass = (basicAuthInfo).Split(':');
                    if (userPass.Length != 2)
                    {
                        throw new ArgumentException($"Configuration property {SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthUserInfo} must be of the form 'username:password'.");
                    }
                    username = userPass[0];
                    password = userPass[1];
                }
            }
            else if (basicAuthSource == "SASL_INHERIT")
            {
                if (basicAuthInfo != "")
                {
                    throw new ArgumentException($"{SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthCredentialsSource} set to 'SASL_INHERIT', but {SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthUserInfo} as also specified.");
                }
                var saslUsername = config.FirstOrDefault(prop => prop.Key == "sasl.username");
                var saslPassword = config.FirstOrDefault(prop => prop.Key == "sasl.password");
                if (saslUsername.Value == null)
                {
                    throw new ArgumentException($"{SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthCredentialsSource} set to 'SASL_INHERIT', but 'sasl.username' property not specified.");
                }
                if (saslPassword.Value == null)
                {
                    throw new ArgumentException($"{SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthCredentialsSource} set to 'SASL_INHERIT', but 'sasl.password' property not specified.");
                }
                username = Convert.ToString(saslUsername.Value);
                password = Convert.ToString(saslPassword.Value);
            }
            else
            {
                throw new ArgumentException($"Invalid value '{basicAuthSource}' specified for property '{SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthCredentialsSource}'");
            }

            foreach (var property in config)
            {
                if (!property.Key.StartsWith("schema.registry."))
                {
                    continue;
                }

                if (property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryUrl &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryRequestTimeoutMs &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryMaxCachedSchemas &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthCredentialsSource &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthUserInfo)
                {
                    throw new ArgumentException($"Unknown configuration parameter {property.Key}");
                }
            }

            this.restService = new RestService(schemaRegistryUris, timeoutMs, username, password);
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
        public async Task<int> GetSchemaIdAsync(string subject, string schema)
        {
            await cacheMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
            try
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

                    schemaId = (await restService.CheckSchemaAsync(subject, schema, true).ConfigureAwait(continueOnCapturedContext: false)).Id;
                    idBySchema[schema] = schemaId;
                    schemaById[schemaId] = schema;
                }

                return schemaId;
            }
            finally
            {
                cacheMutex.Release();
            }
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.SchemaRegistry.ISchemaRegistryClient.RegisterSchemaAsync(string, string)" />
        /// </summary>
        public async Task<int> RegisterSchemaAsync(string subject, string schema)
        {
            await cacheMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
            try
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

                    schemaId = await restService.RegisterSchemaAsync(subject, schema).ConfigureAwait(continueOnCapturedContext: false);
                    idBySchema[schema] = schemaId;
                    schemaById[schemaId] = schema;
                }

                return schemaId;
            }
            finally
            {
                cacheMutex.Release();
            }
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.SchemaRegistry.ISchemaRegistryClient.GetSchemaAsync(int)" />
        /// </summary>
        public async Task<string> GetSchemaAsync(int id)
        {
            await cacheMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
            try
            {
                if (!this.schemaById.TryGetValue(id, out string schema))
                {
                    CleanCacheIfFull();

                    schema = await restService.GetSchemaAsync(id).ConfigureAwait(continueOnCapturedContext: false);
                    schemaById[id] = schema;
                }

                return schema;
            }
            finally
            {
                cacheMutex.Release();
            }
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.SchemaRegistry.ISchemaRegistryClient.GetSchemaAsync(string, int)" />
        /// </summary>
        public async Task<string> GetSchemaAsync(string subject, int version)
        {
            await cacheMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
            try
            {
                CleanCacheIfFull();

                if (!schemaByVersionBySubject.TryGetValue(subject, out Dictionary<int, string> schemaByVersion))
                {
                    schemaByVersion = new Dictionary<int, string>();
                    schemaByVersionBySubject[subject] = schemaByVersion;
                }

                if (!schemaByVersion.TryGetValue(version, out string schemaString))
                {
                    var schema = await restService.GetSchemaAsync(subject, version).ConfigureAwait(continueOnCapturedContext: false);
                    schemaString = schema.SchemaString;
                    schemaByVersion[version] = schemaString;
                    schemaById[schema.Id] = schemaString;
                }

                return schemaString;
            }
            finally
            {
                cacheMutex.Release();
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
        ///     Refer to <see cref="Confluent.SchemaRegistry.ISchemaRegistryClient.GetSubjectVersionsAsync(string)" />
        /// </summary>
        public async Task<List<int>> GetSubjectVersionsAsync(string subject)
            => await restService.GetSubjectVersionsAsync(subject).ConfigureAwait(continueOnCapturedContext: false);


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
