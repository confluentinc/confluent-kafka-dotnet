// Copyright 2016-2020 Confluent Inc.
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

// Disable obsolete warnings. ConstructValueSubjectName is still used a an internal implementation detail.
#pragma warning disable CS0618
#pragma warning disable CS0612

using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using System;
using System.Threading;
using System.Security.Cryptography.X509Certificates;
using Confluent.Kafka;


namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     A caching Schema Registry client.
    ///
    ///     The following method calls cache results:
    ///      - <see cref="CachedSchemaRegistryClient.GetSchemaIdAsync(string, Schema, bool)" />
    ///      - <see cref="CachedSchemaRegistryClient.GetSchemaIdAsync(string, string, bool)" />
    ///      - <see cref="CachedSchemaRegistryClient.GetSchemaAsync(int, string)" />
    ///      - <see cref="CachedSchemaRegistryClient.RegisterSchemaAsync(string, Schema, bool)" />
    ///      - <see cref="CachedSchemaRegistryClient.RegisterSchemaAsync(string, string, bool)" />
    ///      - <see cref="CachedSchemaRegistryClient.GetRegisteredSchemaAsync(string, int)" />
    ///
    ///     The following method calls do NOT cache results:
    ///      - <see cref="CachedSchemaRegistryClient.LookupSchemaAsync(string, Schema, bool, bool)" />
    ///      - <see cref="CachedSchemaRegistryClient.GetLatestSchemaAsync(string)" />
    ///      - <see cref="CachedSchemaRegistryClient.GetAllSubjectsAsync" />
    ///      - <see cref="CachedSchemaRegistryClient.GetSubjectVersionsAsync(string)" />
    ///      - <see cref="CachedSchemaRegistryClient.IsCompatibleAsync(string, Schema)" />
    ///      - <see cref="CachedSchemaRegistryClient.IsCompatibleAsync(string, string)" />
    ///      - <see cref="CachedSchemaRegistryClient.GetCompatibilityAsync(string)" />
    ///      - <see cref="CachedSchemaRegistryClient.UpdateCompatibilityAsync(Compatibility, string)" />
    /// </summary>
    public class CachedSchemaRegistryClient : ISchemaRegistryClient, IDisposable
    {
        private readonly List<SchemaReference> EmptyReferencesList = new List<SchemaReference>();

        private IRestService restService;
        private int identityMapCapacity;
        private readonly Dictionary<int, Schema> schemaById = new Dictionary<int, Schema>();

        private readonly Dictionary<string /*subject*/, Dictionary<string, int>> idBySchemaBySubject = new Dictionary<string, Dictionary<string, int>>();
        private readonly Dictionary<string /*subject*/, Dictionary<int, RegisteredSchema>> schemaByVersionBySubject = new Dictionary<string, Dictionary<int, RegisteredSchema>>();

        private readonly SemaphoreSlim cacheMutex = new SemaphoreSlim(1);

        private SubjectNameStrategyDelegate keySubjectNameStrategy;
        private SubjectNameStrategyDelegate valueSubjectNameStrategy;


        /// <summary>
        ///     The default timeout value for Schema Registry REST API calls.
        /// </summary>
        public const int DefaultTimeout = 30000;

        /// <summary>
        ///     The default maximum capacity of the local schema cache.
        /// </summary>
        public const int DefaultMaxCachedSchemas = 1000;

        /// <summary>
        ///     The default SSL server certificate verification for Schema Registry REST API calls.
        /// </summary>
        public const bool DefaultEnableSslCertificateVerification = true;

        /// <summary>
        ///     The default key subject name strategy.
        /// </summary>
        public const SubjectNameStrategy DefaultKeySubjectNameStrategy = SubjectNameStrategy.Topic;

        /// <summary>
        ///     The default value subject name strategy.
        /// </summary>
        public const SubjectNameStrategy DefaultValueSubjectNameStrategy = SubjectNameStrategy.Topic;

        /// <inheritdoc />
        public int MaxCachedSchemas
            => identityMapCapacity;


        [Obsolete]
        private static SubjectNameStrategyDelegate GetKeySubjectNameStrategy(IEnumerable<KeyValuePair<string, string>> config)
        {
            var keySubjectNameStrategyString = config.FirstOrDefault(prop => prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryKeySubjectNameStrategy).Value ?? "";
            SubjectNameStrategy keySubjectNameStrategy = SubjectNameStrategy.Topic;
            if (keySubjectNameStrategyString != "" &&
                !Enum.TryParse<SubjectNameStrategy>(keySubjectNameStrategyString, out keySubjectNameStrategy))
            {
                throw new ArgumentException($"Unknown KeySubjectNameStrategy: {keySubjectNameStrategyString}");
            }
            return keySubjectNameStrategy.ToDelegate();
        }


        [Obsolete]
        private static SubjectNameStrategyDelegate GetValueSubjectNameStrategy(IEnumerable<KeyValuePair<string, string>> config)
        {
            var valueSubjectNameStrategyString = config.FirstOrDefault(prop => prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryValueSubjectNameStrategy).Value ?? "";
            SubjectNameStrategy valueSubjectNameStrategy = SubjectNameStrategy.Topic;
            if (valueSubjectNameStrategyString != "" &&
                !Enum.TryParse<SubjectNameStrategy>(valueSubjectNameStrategyString, out valueSubjectNameStrategy))
            {
                throw new ArgumentException($"Unknown ValueSubjectNameStrategy: {valueSubjectNameStrategyString}");
            }
            return valueSubjectNameStrategy.ToDelegate();
        }

        /// <summary>
        ///     Initialize a new instance of the SchemaRegistryClient class with a custom <see cref="IAuthenticationHeaderValueProvider"/>
        /// </summary>
        /// <param name="config">
        ///     Configuration properties.
        /// </param>
        /// <param name="authenticationHeaderValueProvider">
        ///     The authentication header value provider
        /// </param>
        public CachedSchemaRegistryClient(IEnumerable<KeyValuePair<string, string>> config, IAuthenticationHeaderValueProvider authenticationHeaderValueProvider)
        {
            if (config == null)
            {
                throw new ArgumentNullException("config properties must be specified.");
            }

            keySubjectNameStrategy = GetKeySubjectNameStrategy(config);
            valueSubjectNameStrategy = GetValueSubjectNameStrategy(config);

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

            var basicAuthSource = config.FirstOrDefault(prop => prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthCredentialsSource).Value ?? "";
            var basicAuthInfo = config.FirstOrDefault(prop => prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthUserInfo).Value ?? "";

            string username = null;
            string password = null;

            if (basicAuthSource == "USER_INFO" || basicAuthSource == "")
            {
                if (basicAuthInfo != "")
                {
                    var userPass = basicAuthInfo.Split(new char[] { ':' }, 2);
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
                username = saslUsername.Value;
                password = saslPassword.Value;
            }
            else
            {
                throw new ArgumentException($"Invalid value '{basicAuthSource}' specified for property '{SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthCredentialsSource}'");
            }

            if (authenticationHeaderValueProvider != null)
            {
                if (username != null || password != null)
                {
                    throw new ArgumentException($"Invalid authentication header value provider configuration: Cannot specify both custom provider and username/password");
                }
            }
            else
            {
                if (username != null && password == null)
                {
                    throw new ArgumentException($"Invalid authentication header value provider configuration: Basic authentication username specified, but password not specified");
                }
                if (username == null && password != null)
                {
                    throw new ArgumentException($"Invalid authentication header value provider configuration: Basic authentication password specified, but username not specified");
                }
                else if (username != null && password != null)
                {
                    authenticationHeaderValueProvider = new BasicAuthenticationHeaderValueProvider(username, password);
                }
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
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthUserInfo &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryKeySubjectNameStrategy &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryValueSubjectNameStrategy &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SslCaLocation &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SslKeystoreLocation &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SslKeystorePassword &&
                    property.Key != SchemaRegistryConfig.PropertyNames.EnableSslCertificateVerification)
                {
                    throw new ArgumentException($"Unknown configuration parameter {property.Key}");
                }
            }

            var sslVerificationMaybe = config.FirstOrDefault(prop => prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.EnableSslCertificateVerification);
            bool sslVerify;
            try { sslVerify = sslVerificationMaybe.Value == null ? DefaultEnableSslCertificateVerification : bool.Parse(sslVerificationMaybe.Value); }
            catch (FormatException) { throw new ArgumentException($"Configured value for {SchemaRegistryConfig.PropertyNames.EnableSslCertificateVerification} must be a bool."); }

            this.restService = new RestService(schemaRegistryUris, timeoutMs, authenticationHeaderValueProvider, SetSslConfig(config), sslVerify);
        }

        /// <summary>
        ///     Initialize a new instance of the SchemaRegistryClient class.
        /// </summary>
        /// <param name="config">
        ///     Configuration properties.
        /// </param>
        public CachedSchemaRegistryClient(IEnumerable<KeyValuePair<string, string>> config)
            : this (config, null)
        {

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
        ///     Add certificates for SSL handshake.
        /// </summary>
        /// <param name="config">
        ///     Configuration properties.
        /// </param>
        private List<X509Certificate2> SetSslConfig(IEnumerable<KeyValuePair<string, string>> config)
        {
            var certificates = new List<X509Certificate2>();

            var certificateLocation = config.FirstOrDefault(prop => prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SslKeystoreLocation).Value ?? "";
            var certificatePassword = config.FirstOrDefault(prop => prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SslKeystorePassword).Value ?? "";
            if (!String.IsNullOrEmpty(certificateLocation))
            {
                certificates.Add(new X509Certificate2(certificateLocation, certificatePassword));
            }

            var caLocation = config.FirstOrDefault(prop => prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SslCaLocation).Value ?? "";
            if (!String.IsNullOrEmpty(caLocation))
            {
                certificates.Add(new X509Certificate2(caLocation));
            }

            return certificates;
        }

        /// <inheritdoc/>
        public Task<int> GetSchemaIdAsync(string subject, string avroSchema, bool normalize = false)
            => GetSchemaIdAsync(subject, new Schema(avroSchema, EmptyReferencesList, SchemaType.Avro), normalize);

        
        /// <inheritdoc/>
        public async Task<int> GetSchemaIdAsync(string subject, Schema schema, bool normalize = false)
        {
            await cacheMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
            try
            {
                if (!this.idBySchemaBySubject.TryGetValue(subject, out Dictionary<string, int> idBySchema))
                {
                    idBySchema = new Dictionary<string, int>();
                    this.idBySchemaBySubject.Add(subject, idBySchema);
                }

                // TODO: The following could be optimized in the usual case where idBySchema only
                // contains very few elements and the schema string passed in is always the same
                // instance.

                if (!idBySchema.TryGetValue(schema.SchemaString, out int schemaId))
                {
                    CleanCacheIfFull();

                    // throws SchemaRegistryException if schema is not known.
                    var registeredSchema = await restService.LookupSchemaAsync(subject, schema, true, normalize).ConfigureAwait(continueOnCapturedContext: false);
                    idBySchema[schema.SchemaString] = registeredSchema.Id;
                    schemaById[registeredSchema.Id] = registeredSchema.Schema;
                    schemaId = registeredSchema.Id;
                }

                return schemaId;
            }
            finally
            {
                cacheMutex.Release();
            }
        }


        /// <inheritdoc/>
        public async Task<int> RegisterSchemaAsync(string subject, Schema schema, bool normalize = false)
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

                if (!idBySchema.TryGetValue(schema.SchemaString, out int schemaId))
                {
                    CleanCacheIfFull();

                    schemaId = await restService.RegisterSchemaAsync(subject, schema, normalize).ConfigureAwait(continueOnCapturedContext: false);
                    idBySchema[schema.SchemaString] = schemaId;
                }

                return schemaId;
            }
            finally
            {
                cacheMutex.Release();
            }
        }


        /// <inheritdoc/>
        public Task<int> RegisterSchemaAsync(string subject, string avroSchema, bool normalize = false)
            => RegisterSchemaAsync(subject, new Schema(avroSchema, EmptyReferencesList, SchemaType.Avro), normalize);
    

        /// <summary>
        ///     Check if the given schema string matches a given format name.
        /// </summary>
        private bool checkSchemaMatchesFormat(string format, string schemaString)
        {
            // if a format isn't specified, then assume text is desired.
            if (format == null)
            {
                try { Convert.FromBase64String(schemaString); }
                catch (Exception)
                {
                    return true; // Base64 conversion failed, infer the schemaString format is text.
                }

                return false; // Base64 conversion succeeded, so infer the schamaString format is base64.
            }
            else
            {
                if (format != "serialized")
                {
                    throw new ArgumentException($"Invalid schema format was specified: {format}.");
                }

                try { Convert.FromBase64String(schemaString); }
                catch (Exception)
                {
                    return false;
                }
                return true;
            }
        }


        /// <inheritdoc/>
        public Task<RegisteredSchema> LookupSchemaAsync(string subject, Schema schema, bool ignoreDeletedSchemas, bool normalize = false)
            => restService.LookupSchemaAsync(subject, schema, ignoreDeletedSchemas, normalize);


        /// <inheritdoc/>
        public async Task<Schema> GetSchemaAsync(int id, string format = null)
        {
            await cacheMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
            try
            {
                if (!this.schemaById.TryGetValue(id, out Schema schema) || !checkSchemaMatchesFormat(format, schema.SchemaString))
                {
                    CleanCacheIfFull();
                    schema = (await restService.GetSchemaAsync(id, format).ConfigureAwait(continueOnCapturedContext: false));
                    schemaById[id] = schema;
                }

                return schema;
            }
            finally
            {
                cacheMutex.Release();
            }
        }


        /// <inheritdoc/>
        public async Task<RegisteredSchema> GetRegisteredSchemaAsync(string subject, int version)
        {
            await cacheMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
            try
            {
                CleanCacheIfFull();

                if (!schemaByVersionBySubject.TryGetValue(subject, out Dictionary<int, RegisteredSchema> schemaByVersion))
                {
                    schemaByVersion = new Dictionary<int, RegisteredSchema>();
                    schemaByVersionBySubject[subject] = schemaByVersion;
                }

                if (!schemaByVersion.TryGetValue(version, out RegisteredSchema schema))
                {
                    schema = await restService.GetSchemaAsync(subject, version).ConfigureAwait(continueOnCapturedContext: false);
                    schemaByVersion[version] = schema;
                    schemaById[schema.Id] = schema.Schema;
                }

                return schema;
            }
            finally
            {
                cacheMutex.Release();
            }
        }


        /// <inheritdoc/>
        [Obsolete("Superseded by GetRegisteredSchemaAsync(string subject, int version). This method will be removed in a future release.")]
        public async Task<string> GetSchemaAsync(string subject, int version)
            => (await GetRegisteredSchemaAsync(subject, version)).SchemaString;


        /// <inheritdoc/>
        public async Task<RegisteredSchema> GetLatestSchemaAsync(string subject)
            => await restService.GetLatestSchemaAsync(subject).ConfigureAwait(continueOnCapturedContext: false);


        /// <inheritdoc/>
        public Task<List<string>> GetAllSubjectsAsync()
            => restService.GetSubjectsAsync();


        /// <inheritdoc/>
        public async Task<List<int>> GetSubjectVersionsAsync(string subject)
            => await restService.GetSubjectVersionsAsync(subject).ConfigureAwait(continueOnCapturedContext: false);


        /// <inheritdoc/>
        public async Task<bool> IsCompatibleAsync(string subject, Schema schema)
            => await restService.TestLatestCompatibilityAsync(subject, schema).ConfigureAwait(continueOnCapturedContext: false);


        /// <inheritdoc/>
        public async Task<bool> IsCompatibleAsync(string subject, string avroSchema)
            => await restService.TestLatestCompatibilityAsync(subject, new Schema(avroSchema, EmptyReferencesList, SchemaType.Avro)).ConfigureAwait(continueOnCapturedContext: false);


        /// <inheritdoc />
        [Obsolete("SubjectNameStrategy should now be specified via serializer configuration. This method will be removed in a future release.")]
        public string ConstructKeySubjectName(string topic, string recordType = null)
            => keySubjectNameStrategy(new SerializationContext(MessageComponentType.Key, topic), recordType);


        /// <inheritdoc />
        [Obsolete("SubjectNameStrategy should now be specified via serializer configuration. This method will be removed in a future release.")]
        public string ConstructValueSubjectName(string topic, string recordType = null)
            => valueSubjectNameStrategy(new SerializationContext(MessageComponentType.Value, topic), recordType);

        /// <inheritdoc />
        public async Task<Compatibility> GetCompatibilityAsync(string subject = null)
            => await restService.GetCompatibilityAsync(subject)
                    .ConfigureAwait(continueOnCapturedContext: false);

        /// <inheritdoc />
        public async Task<Compatibility> UpdateCompatibilityAsync(Compatibility compatibility, string subject = null)
            => await restService.UpdateCompatibilityAsync(subject, compatibility)
                .ConfigureAwait(continueOnCapturedContext: false);


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
