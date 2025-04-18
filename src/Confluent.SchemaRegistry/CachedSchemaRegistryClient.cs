﻿// Copyright 2016-2020 Confluent Inc.
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
using System.Net.Http;
using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using Confluent.Kafka;
using Confluent.Shared.CollectionUtils;
using Microsoft.Extensions.Caching.Memory;


namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     A caching Schema Registry client.
    ///
    ///     The following method calls cache results:
    ///      - <see cref="CachedSchemaRegistryClient.GetSchemaIdAsync(string, Schema, bool)" />
    ///      - <see cref="CachedSchemaRegistryClient.GetSchemaIdAsync(string, string, bool)" />
    ///      - <see cref="CachedSchemaRegistryClient.GetSchemaAsync(int, string)" />
    ///      - <see cref="CachedSchemaRegistryClient.GetSchemaBySubjectAndIdAsync(string, int, string)" />
    ///      - <see cref="CachedSchemaRegistryClient.RegisterSchemaAsync(string, Schema, bool)" />
    ///      - <see cref="CachedSchemaRegistryClient.RegisterSchemaAsync(string, string, bool)" />
    ///      - <see cref="CachedSchemaRegistryClient.GetRegisteredSchemaAsync(string, int, bool)" />
    ///      - <see cref="CachedSchemaRegistryClient.LookupSchemaAsync(string, Schema, bool, bool)" />
    ///
    ///     The following method calls do NOT cache results:
    ///      - <see cref="CachedSchemaRegistryClient.GetLatestSchemaAsync(string)" />
    ///      - <see cref="CachedSchemaRegistryClient.GetAllSubjectsAsync" />
    ///      - <see cref="CachedSchemaRegistryClient.GetSubjectVersionsAsync(string)" />
    ///      - <see cref="CachedSchemaRegistryClient.IsCompatibleAsync(string, Schema)" />
    ///      - <see cref="CachedSchemaRegistryClient.IsCompatibleAsync(string, string)" />
    ///      - <see cref="CachedSchemaRegistryClient.GetCompatibilityAsync(string)" />
    ///      - <see cref="CachedSchemaRegistryClient.UpdateCompatibilityAsync(Compatibility, string)" />
    /// </summary>
    public class CachedSchemaRegistryClient : ISchemaRegistryClient
    {
        private record struct SchemaId(int Id, string Format);
        
        private readonly List<SchemaReference> EmptyReferencesList = new List<SchemaReference>();

        private IEnumerable<KeyValuePair<string, string>> config;
        private IAuthenticationHeaderValueProvider authHeaderProvider;
        private IWebProxy proxy;

        private IRestService restService;
        private int identityMapCapacity;
        private int latestCacheTtlSecs;
        private readonly ConcurrentDictionary<SchemaId, Task<Schema>> schemaById = new ConcurrentDictionary<SchemaId, Task<Schema>>();

        private readonly ConcurrentDictionary<string /*subject*/, ConcurrentDictionary<Schema, Task<int>>> idBySchemaBySubject =
            new ConcurrentDictionary<string, ConcurrentDictionary<Schema, Task<int>>>();

        private readonly ConcurrentDictionary<string /*subject*/, ConcurrentDictionary<int, Task<RegisteredSchema>>> schemaByVersionBySubject =
            new ConcurrentDictionary<string, ConcurrentDictionary<int, Task<RegisteredSchema>>>();
        
        private readonly ConcurrentDictionary<string /*subject*/, ConcurrentDictionary<Schema, Task<RegisteredSchema>>> registeredSchemaBySchemaBySubject =
            new ConcurrentDictionary<string, ConcurrentDictionary<Schema, Task<RegisteredSchema>>>();

        private readonly MemoryCache latestVersionBySubject = new MemoryCache(new MemoryCacheOptions());
        
        private readonly MemoryCache latestWithMetadataBySubject = new MemoryCache(new MemoryCacheOptions());

        private SubjectNameStrategyDelegate keySubjectNameStrategy;
        private SubjectNameStrategyDelegate valueSubjectNameStrategy;


        /// <summary>
        ///     The default timeout value for Schema Registry REST API calls.
        /// </summary>
        public const int DefaultTimeout = 30000;

        /// <summary>
        ///     The default maximum number of retries.
        /// </summary>
        public const int DefaultMaxRetries = RestService.DefaultMaxRetries;

        /// <summary>
        ///     The default time to wait for the first retry.
        /// </summary>
        public const int DefaultRetriesWaitMs = RestService.DefaultRetriesWaitMs;

        /// <summary>
        ///     The default time to wait for any retry.
        /// </summary>
        public const int DefaultRetriesMaxWaitMs = RestService.DefaultRetriesMaxWaitMs;

        /// <summary>
        ///     The default maximum capacity of the local schema cache.
        /// </summary>
        public const int DefaultMaxCachedSchemas = 1000;

        /// <summary>
        ///     The default TTL for caches holding latest schemas.
        /// </summary>
        public const int DefaultLatestCacheTtlSecs = -1;

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
        public IEnumerable<KeyValuePair<string, string>> Config
            => config;


        /// <inheritdoc />
        public IAuthenticationHeaderValueProvider AuthHeaderProvider
            => authHeaderProvider;


        /// <inheritdoc />
        public IWebProxy Proxy
            => proxy;


        /// <inheritdoc />
        public int MaxCachedSchemas
            => identityMapCapacity;


        [Obsolete]
        private static SubjectNameStrategyDelegate GetKeySubjectNameStrategy(
            IEnumerable<KeyValuePair<string, string>> config)
        {
            var keySubjectNameStrategyString = config.FirstOrDefault(prop =>
                                                   prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames
                                                       .SchemaRegistryKeySubjectNameStrategy).Value ??
                                               "";
            SubjectNameStrategy keySubjectNameStrategy = SubjectNameStrategy.Topic;
            if (keySubjectNameStrategyString != "" &&
                !Enum.TryParse<SubjectNameStrategy>(keySubjectNameStrategyString, out keySubjectNameStrategy))
            {
                throw new ArgumentException($"Unknown KeySubjectNameStrategy: {keySubjectNameStrategyString}");
            }

            return keySubjectNameStrategy.ToDelegate();
        }


        [Obsolete]
        private static SubjectNameStrategyDelegate GetValueSubjectNameStrategy(
            IEnumerable<KeyValuePair<string, string>> config)
        {
            var valueSubjectNameStrategyString = config.FirstOrDefault(prop =>
                    prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryValueSubjectNameStrategy)
                .Value ?? "";
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
        /// <param name="proxy">
        ///     The proxy server to use for connections
        /// </param>
        public CachedSchemaRegistryClient(IEnumerable<KeyValuePair<string, string>> config,
            IAuthenticationHeaderValueProvider authenticationHeaderValueProvider,
            IWebProxy proxy = null)
        {
            if (config == null)
            {
                throw new ArgumentNullException("config");
            }
            
            this.config = config;
            this.authHeaderProvider = authenticationHeaderValueProvider;
            this.proxy = proxy;

            keySubjectNameStrategy = GetKeySubjectNameStrategy(config);
            valueSubjectNameStrategy = GetValueSubjectNameStrategy(config);

            var schemaRegistryUrisMaybe = config.FirstOrDefault(prop =>
                prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryUrl);
            if (schemaRegistryUrisMaybe.Value == null)
            {
                throw new ArgumentException(
                    $"{SchemaRegistryConfig.PropertyNames.SchemaRegistryUrl} configuration property must be specified.");
            }

            var schemaRegistryUris = (string)schemaRegistryUrisMaybe.Value;

            var timeoutMsMaybe = config.FirstOrDefault(prop =>
                prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryRequestTimeoutMs);
            int timeoutMs;
            try
            {
                timeoutMs = timeoutMsMaybe.Value == null ? DefaultTimeout : Convert.ToInt32(timeoutMsMaybe.Value);
            }
            catch (FormatException)
            {
                throw new ArgumentException(
                    $"Configured value for {SchemaRegistryConfig.PropertyNames.SchemaRegistryRequestTimeoutMs} must be an integer.");
            }

            var maxRetriesMaybe = config.FirstOrDefault(prop =>
                prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryMaxRetries);
            int maxRetries;
            try
            {
                maxRetries = maxRetriesMaybe.Value == null ? DefaultMaxRetries : Convert.ToInt32(maxRetriesMaybe.Value);
            }
            catch (FormatException)
            {
                throw new ArgumentException(
                    $"Configured value for {SchemaRegistryConfig.PropertyNames.SchemaRegistryMaxRetries} must be an integer.");
            }

            var retriesWaitMsMaybe = config.FirstOrDefault(prop =>
                prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryRetriesWaitMs);
            int retriesWaitMs;
            try
            {
                retriesWaitMs = retriesWaitMsMaybe.Value == null ? DefaultRetriesWaitMs : Convert.ToInt32(retriesWaitMsMaybe.Value);
            }
            catch (FormatException)
            {
                throw new ArgumentException(
                    $"Configured value for {SchemaRegistryConfig.PropertyNames.SchemaRegistryRetriesWaitMs} must be an integer.");
            }

            var retriesMaxWaitMsMaybe = config.FirstOrDefault(prop =>
                prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryRetriesMaxWaitMs);
            int retriesMaxWaitMs;
            try
            {
                retriesMaxWaitMs = retriesMaxWaitMsMaybe.Value == null ? DefaultRetriesMaxWaitMs : Convert.ToInt32(retriesMaxWaitMsMaybe.Value);
            }
            catch (FormatException)
            {
                throw new ArgumentException(
                    $"Configured value for {SchemaRegistryConfig.PropertyNames.SchemaRegistryRetriesMaxWaitMs} must be an integer.");
            }

            var identityMapCapacityMaybe = config.FirstOrDefault(prop =>
                prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryMaxCachedSchemas);
            try
            {
                this.identityMapCapacity = identityMapCapacityMaybe.Value == null
                    ? DefaultMaxCachedSchemas
                    : Convert.ToInt32(identityMapCapacityMaybe.Value);
            }
            catch (FormatException)
            {
                throw new ArgumentException(
                    $"Configured value for {SchemaRegistryConfig.PropertyNames.SchemaRegistryMaxCachedSchemas} must be an integer.");
            }

            var latestCacheTtlSecsMaybe = config.FirstOrDefault(prop =>
                prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryLatestCacheTtlSecs);
            try
            {
                this.latestCacheTtlSecs = latestCacheTtlSecsMaybe.Value == null
                    ? DefaultLatestCacheTtlSecs
                    : Convert.ToInt32(latestCacheTtlSecsMaybe.Value);
            }
            catch (FormatException)
            {
                throw new ArgumentException(
                    $"Configured value for {SchemaRegistryConfig.PropertyNames.SchemaRegistryLatestCacheTtlSecs} must be an integer.");
            }
            
            var basicAuthSource = config.FirstOrDefault(prop =>
                    prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthCredentialsSource)
                .Value ?? "";
            var basicAuthInfo = config.FirstOrDefault(prop =>
                prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthUserInfo).Value ?? "";

            string username = null;
            string password = null;

            if (basicAuthSource == "USER_INFO" || basicAuthSource == "")
            {
                if (basicAuthInfo != "")
                {
                    var userPass = basicAuthInfo.Split(new char[] { ':' }, 2);
                    if (userPass.Length != 2)
                    {
                        throw new ArgumentException(
                            $"Configuration property {SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthUserInfo} must be of the form 'username:password'.");
                    }

                    username = userPass[0];
                    password = userPass[1];
                    if (authenticationHeaderValueProvider != null)
                    {
                        throw new ArgumentException(
                            $"Invalid authentication header value provider configuration: Cannot specify both custom provider and username/password");
                    }
                    authenticationHeaderValueProvider = new BasicAuthenticationHeaderValueProvider(username, password);
                }
            }
            else if (basicAuthSource == "SASL_INHERIT")
            {
                if (basicAuthInfo != "")
                {
                    throw new ArgumentException(
                        $"{SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthCredentialsSource} set to 'SASL_INHERIT', but {SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthUserInfo} as also specified.");
                }

                var saslUsername = config.FirstOrDefault(prop => prop.Key == "sasl.username");
                var saslPassword = config.FirstOrDefault(prop => prop.Key == "sasl.password");
                if (saslUsername.Value == null)
                {
                    throw new ArgumentException(
                        $"{SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthCredentialsSource} set to 'SASL_INHERIT', but 'sasl.username' property not specified.");
                }

                if (saslPassword.Value == null)
                {
                    throw new ArgumentException(
                        $"{SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthCredentialsSource} set to 'SASL_INHERIT', but 'sasl.password' property not specified.");
                }

                username = saslUsername.Value;
                password = saslPassword.Value;
                if (authenticationHeaderValueProvider != null)
                {
                    throw new ArgumentException(
                        $"Invalid authentication header value provider configuration: Cannot specify both custom provider and username/password");
                }
                authenticationHeaderValueProvider = new BasicAuthenticationHeaderValueProvider(username, password);
            }
            else
            {
                throw new ArgumentException(
                    $"Invalid value '{basicAuthSource}' specified for property '{SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthCredentialsSource}'");
            }

            var bearerAuthSource = config.FirstOrDefault(prop =>
                prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthCredentialsSource).Value ?? "";

            if (bearerAuthSource != "" && basicAuthSource != "")
            {
                throw new ArgumentException(
                    $"Invalid authentication header value provider configuration: Cannot specify both basic and bearer authentication");
            }

            string logicalCluster = null;
            string identityPoolId = null;
            string bearerToken = null;
            string clientId = null;
            string clientSecret = null;
            string scope = null;
            string tokenEndpointUrl = null;

            if (bearerAuthSource == "STATIC_TOKEN" || bearerAuthSource == "OAUTHBEARER")
            {
                if (authenticationHeaderValueProvider != null)
                {
                    throw new ArgumentException(
                        $"Invalid authentication header value provider configuration: Cannot specify both custom provider and bearer authentication");
                }
                logicalCluster = config.FirstOrDefault(prop =>
                    prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthLogicalCluster).Value;

                identityPoolId = config.FirstOrDefault(prop =>
                    prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthIdentityPoolId).Value;
                if (logicalCluster == null || identityPoolId == null)
                {
                    throw new ArgumentException(
                        $"Invalid bearer authentication provider configuration: Logical cluster and identity pool ID must be specified");
                }
            }

            switch (bearerAuthSource)
            {
                case "STATIC_TOKEN":
                    bearerToken = config.FirstOrDefault(prop =>
                        prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthToken).Value;

                    if (bearerToken == null)
                    {
                        throw new ArgumentException(
                            $"Invalid authentication header value provider configuration: Bearer authentication token not specified");
                    }
                    authenticationHeaderValueProvider = new StaticBearerAuthenticationHeaderValueProvider(bearerToken, logicalCluster, identityPoolId);
                    break;

                case "OAUTHBEARER":
                    clientId = config.FirstOrDefault(prop =>
                        prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthClientId).Value;

                    clientSecret = config.FirstOrDefault(prop =>
                        prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthClientSecret).Value;

                    scope = config.FirstOrDefault(prop =>
                        prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthScope).Value;
                    
                    tokenEndpointUrl = config.FirstOrDefault(prop =>
                        prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthTokenEndpointUrl).Value;
                    
                    if (tokenEndpointUrl == null || clientId == null || clientSecret == null || scope == null)
                    {
                        throw new ArgumentException(
                            $"Invalid bearer authentication provider configuration: Token endpoint URL, client ID, client secret, and scope must be specified");
                    }
                    authenticationHeaderValueProvider = new BearerAuthenticationHeaderValueProvider(
                        new HttpClient(), clientId, clientSecret, scope, tokenEndpointUrl, logicalCluster, identityPoolId, maxRetries, retriesWaitMs, retriesMaxWaitMs);
                    break;

                case "CUSTOM":
                    if (authenticationHeaderValueProvider == null)
                    {
                        throw new ArgumentException(
                            $"Invalid authentication header value provider configuration: Custom authentication provider must be specified");
                    }
                    if(!(authenticationHeaderValueProvider is IAuthenticationBearerHeaderValueProvider))
                    {
                        throw new ArgumentException(
                            $"Invalid authentication header value provider configuration: Custom authentication provider must implement IAuthenticationBearerHeaderValueProvider");
                    }
                    break;

                case "":
                    break;

                default:
                    throw new ArgumentException(
                        $"Invalid value '{bearerAuthSource}' specified for property '{SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthCredentialsSource}'");
            }

            foreach (var property in config)
            {
                if (!property.Key.StartsWith("schema.registry."))
                {
                    continue;
                }

                if (property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryUrl &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryRequestTimeoutMs &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryMaxRetries &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryRetriesWaitMs &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryRetriesMaxWaitMs &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryMaxCachedSchemas &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryLatestCacheTtlSecs &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthCredentialsSource &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthUserInfo &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthCredentialsSource &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthToken &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthClientId &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthClientSecret &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthScope &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthTokenEndpointUrl &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthLogicalCluster &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthIdentityPoolId &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryKeySubjectNameStrategy &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryValueSubjectNameStrategy &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SslCaLocation &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SslKeystoreLocation &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SslKeystorePassword &&
                    property.Key != SchemaRegistryConfig.PropertyNames.EnableSslCertificateVerification)
                {
                    throw new ArgumentException($"Unknown configuration parameter {property.Key}");
                }
            }

            var sslVerificationMaybe = config.FirstOrDefault(prop =>
                prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.EnableSslCertificateVerification);
            bool sslVerify;
            try
            {
                sslVerify = sslVerificationMaybe.Value == null
                    ? DefaultEnableSslCertificateVerification
                    : bool.Parse(sslVerificationMaybe.Value);
            }
            catch (FormatException)
            {
                throw new ArgumentException(
                    $"Configured value for {SchemaRegistryConfig.PropertyNames.EnableSslCertificateVerification} must be a bool.");
            }

            var sslCaLocation = config.FirstOrDefault(prop => prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SslCaLocation).Value;
            var sslCaCertificate = string.IsNullOrEmpty(sslCaLocation) ? null : new X509Certificate2(sslCaLocation);
            this.restService = new RestService(schemaRegistryUris, timeoutMs, authenticationHeaderValueProvider,
                SetSslConfig(config), sslVerify, sslCaCertificate, proxy, maxRetries, retriesWaitMs, retriesMaxWaitMs);
        }

        /// <summary>
        ///     Initialize a new instance of the SchemaRegistryClient class.
        /// </summary>
        /// <param name="config">
        ///     Configuration properties.
        /// </param>
        public CachedSchemaRegistryClient(IEnumerable<KeyValuePair<string, string>> config)
            : this(config, null)
        {
        }

        /// <summary>
        ///     Initialize a new instance of the SchemaRegistryClient class.
        /// </summary>
        /// <param name="config">
        ///     Configuration properties.
        /// </param>
        /// <param name="proxy">
        ///     The proxy server to use for connections
        /// </param>
        public CachedSchemaRegistryClient(IEnumerable<KeyValuePair<string, string>> config, IWebProxy proxy)
            : this(config, null, proxy)
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
                this.registeredSchemaBySchemaBySubject.Clear();
                return true;
            }

            return false;
        }

        /// <summary>
        ///     Add certificates for SSL handshake.
        /// </summary>
        /// <param name="config">
        ///     Configuration properties.
        /// </param>
        private List<X509Certificate2> SetSslConfig(IEnumerable<KeyValuePair<string, string>> config)
        {
            var certificates = new List<X509Certificate2>();

            var certificateLocation = config.FirstOrDefault(prop =>
                prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SslKeystoreLocation).Value ?? "";
            var certificatePassword = config.FirstOrDefault(prop =>
                prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SslKeystorePassword).Value ?? "";
            if (!String.IsNullOrEmpty(certificateLocation))
            {
                certificates.Add(new X509Certificate2(certificateLocation, certificatePassword));
            }

            return certificates;
        }

        /// <inheritdoc/>
        public Task<int> GetSchemaIdAsync(string subject, string avroSchema, bool normalize = false)
            => GetSchemaIdAsync(subject, new Schema(avroSchema, EmptyReferencesList, SchemaType.Avro), normalize);


        /// <inheritdoc/>
        public async Task<int> GetSchemaIdAsync(string subject, Schema schema, bool normalize = false)
        {
            if (idBySchemaBySubject.TryGetValue(subject, out var idBySchema))
            {
                if (idBySchema.TryGetValue(schema, out var schemaId))
                {
                    return await schemaId;
                }
            }
            
            CleanCacheIfFull();

            idBySchema = idBySchemaBySubject.GetOrAdd(subject, _ => new ConcurrentDictionary<Schema, Task<int>>());
            return await idBySchema.GetOrAdd(schema, async _ =>
            {
                var registeredSchema = await LookupSchemaAsync(subject, schema, true, normalize)
                    .ConfigureAwait(continueOnCapturedContext: false);

                // We already have the schema so we can add it to the cache.
                var format = GetSchemaFormat(registeredSchema.SchemaString);
                schemaById.TryAdd(new SchemaId(registeredSchema.Id, format), Task.FromResult(registeredSchema.Schema));
                
                return registeredSchema.Id;
            }).ConfigureAwait(continueOnCapturedContext: false);
        }


        /// <inheritdoc/>
        public async Task<int> RegisterSchemaAsync(string subject, Schema schema, bool normalize = false)
        {
            if (idBySchemaBySubject.TryGetValue(subject, out var idBySchema))
            {
                if (idBySchema.TryGetValue(schema, out var schemaId))
                {
                    return await schemaId;
                }
            }
            
            CleanCacheIfFull();
            idBySchema = idBySchemaBySubject.GetOrAdd(subject, _ => new ConcurrentDictionary<Schema, Task<int>>());
            return await idBySchema.GetOrAddAsync(schema, _ => restService.RegisterSchemaAsync(subject, schema, normalize)).ConfigureAwait(continueOnCapturedContext: false);
        }

        /// <inheritdoc/>
        public Task<int> RegisterSchemaAsync(string subject, string avroSchema, bool normalize = false)
            => RegisterSchemaAsync(subject, new Schema(avroSchema, EmptyReferencesList, SchemaType.Avro), normalize);


        /// <summary>
        ///     Check if the given schema string matches a given format name.
        /// </summary>
        private static string GetSchemaFormat(string schemaString)
        {
            return Utils.IsBase64String(schemaString) ? "serialized" : null;
        }

        /// <inheritdoc/>
        public async Task<RegisteredSchema> LookupSchemaAsync(string subject, Schema schema, bool ignoreDeletedSchemas,
            bool normalize = false)
        {
            if (registeredSchemaBySchemaBySubject.TryGetValue(subject, out var registeredSchemaBySchema))
            {
                if (registeredSchemaBySchema.TryGetValue(schema, out var registeredSchema))
                {
                    return await registeredSchema;
                }
            }
            
            CleanCacheIfFull();
            
            registeredSchemaBySchema = registeredSchemaBySchemaBySubject.GetOrAdd(subject, _ => new ConcurrentDictionary<Schema, Task<RegisteredSchema>>());
            return await registeredSchemaBySchema.GetOrAddAsync(schema, _ => restService.LookupSchemaAsync(subject, schema, ignoreDeletedSchemas, normalize)).ConfigureAwait(continueOnCapturedContext: false);
        }

        /// <inheritdoc/>
        public async Task<Schema> GetSchemaAsync(int id, string format = null)
        {
            var schemaId = new SchemaId(id, format);
            if (schemaById.TryGetValue(schemaId, out var schema))
            {
                return await schema;
            }
            
            CleanCacheIfFull();
            return await schemaById.GetOrAddAsync(schemaId, _ => restService.GetSchemaAsync(id, format)).ConfigureAwait(continueOnCapturedContext: false);
        }


        /// <inheritdoc/>
        public async Task<Schema> GetSchemaBySubjectAndIdAsync(string subject, int id, string format = null)
        {
            var schemaId = new SchemaId(id, format);
            if (this.schemaById.TryGetValue(schemaId, out var schema))
            {
                return await schema;
            }
            
            return await schemaById.GetOrAddAsync(schemaId, _ => restService.GetSchemaBySubjectAndIdAsync(subject, id, format)).ConfigureAwait(continueOnCapturedContext: false);
        }


        /// <inheritdoc/>
        public async Task<RegisteredSchema> GetRegisteredSchemaAsync(string subject, int version, bool ignoreDeletedSchemas = true)
        {
            if (schemaByVersionBySubject.TryGetValue(subject, out var schemaByVersion))
            {
                if (schemaByVersion.TryGetValue(version, out var schema))
                {
                    return await schema;
                }
            }
            
            CleanCacheIfFull();
            schemaByVersion = schemaByVersionBySubject.GetOrAdd(subject, _ => new ConcurrentDictionary<int, Task<RegisteredSchema>>());
            return await schemaByVersion.GetOrAddAsync(version, async _ =>
            {
                var schema = await restService.GetSchemaAsync(subject, version).ConfigureAwait(continueOnCapturedContext: false);
                
                // We already have the schema so we can add it to the cache.
                var format = GetSchemaFormat(schema.SchemaString);
                schemaById.TryAdd(new SchemaId(schema.Id, format), Task.FromResult(schema.Schema));
                
                return schema;
            }).ConfigureAwait(continueOnCapturedContext: false);
        }


        /// <inheritdoc/>
        [Obsolete(
            "Superseded by GetRegisteredSchemaAsync(string subject, int version). This method will be removed in a future release.")]
        public async Task<string> GetSchemaAsync(string subject, int version)
            => (await GetRegisteredSchemaAsync(subject, version)).SchemaString;


        /// <inheritdoc/>
        public async Task<RegisteredSchema> GetLatestSchemaAsync(string subject)
        {
            RegisteredSchema schema;
            if (!latestVersionBySubject.TryGetValue(subject, out schema))
            {
                schema = await restService.GetLatestSchemaAsync(subject).ConfigureAwait(continueOnCapturedContext: false);
                MemoryCacheEntryOptions opts = new MemoryCacheEntryOptions();
                if (latestCacheTtlSecs > 0)
                {
                    opts.AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(latestCacheTtlSecs);
                }

                latestVersionBySubject.Set(subject, schema, opts);
            }
            return schema;
        }

        /// <inheritdoc/>
        public async Task<RegisteredSchema> GetLatestWithMetadataAsync(string subject,
            IDictionary<string, string> metadata, bool ignoreDeletedSchemas)
        {
            var key = (subject, metadata, ignoreDeletedSchemas);
            RegisteredSchema schema;
            if (!latestWithMetadataBySubject.TryGetValue(key, out schema))
            {
                schema =  await restService.GetLatestWithMetadataAsync(subject, metadata, ignoreDeletedSchemas).ConfigureAwait(continueOnCapturedContext: false);
                MemoryCacheEntryOptions opts = new MemoryCacheEntryOptions();
                if (latestCacheTtlSecs > 0)
                {
                    opts.AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(latestCacheTtlSecs);
                }

                latestWithMetadataBySubject.Set(key, schema, opts);
            }
            return schema;
        }

        /// <inheritdoc/>
        public Task<List<string>> GetAllSubjectsAsync()
            => restService.GetSubjectsAsync();


        /// <inheritdoc/>
        public async Task<List<int>> GetSubjectVersionsAsync(string subject)
            => await restService.GetSubjectVersionsAsync(subject).ConfigureAwait(continueOnCapturedContext: false);


        /// <inheritdoc/>
        public async Task<bool> IsCompatibleAsync(string subject, Schema schema)
            => await restService.TestLatestCompatibilityAsync(subject, schema)
                .ConfigureAwait(continueOnCapturedContext: false);


        /// <inheritdoc/>
        public async Task<bool> IsCompatibleAsync(string subject, string avroSchema)
            => await restService
                .TestLatestCompatibilityAsync(subject, new Schema(avroSchema, EmptyReferencesList, SchemaType.Avro))
                .ConfigureAwait(continueOnCapturedContext: false);


        /// <inheritdoc />
        [Obsolete(
            "SubjectNameStrategy should now be specified via serializer configuration. This method will be removed in a future release.")]
        public string ConstructKeySubjectName(string topic, string recordType = null)
            => keySubjectNameStrategy(new SerializationContext(MessageComponentType.Key, topic), recordType);


        /// <inheritdoc />
        [Obsolete(
            "SubjectNameStrategy should now be specified via serializer configuration. This method will be removed in a future release.")]
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
        ///     Clears caches of latest versions.
        /// </summary>
        public void ClearLatestCaches()
        {
            latestVersionBySubject.Clear();
            latestWithMetadataBySubject.Clear();
        }

        /// <summary>
        ///     Clears all caches.
        /// </summary>
        public void ClearCaches()
        {
            schemaById.Clear();
            idBySchemaBySubject.Clear();
            schemaByVersionBySubject.Clear();
            registeredSchemaBySchemaBySubject.Clear();
            latestVersionBySubject.Clear();
            latestWithMetadataBySubject.Clear();
        }

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