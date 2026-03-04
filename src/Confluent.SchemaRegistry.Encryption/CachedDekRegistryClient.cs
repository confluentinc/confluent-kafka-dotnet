// Copyright 2024 Confluent Inc.
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
using System.Net;
using System.Threading;
using System.Security.Cryptography.X509Certificates;

namespace Confluent.SchemaRegistry.Encryption
{
    public record KekId(string Name, bool LookupDeletedKeks);

    public record DekId(string KekName, string Subject, int? Version, DekFormat? DekFormat, bool LookupDeletedDeks);

    /// <summary>
    ///     A caching DEK Registry client.
    /// </summary>
    public class CachedDekRegistryClient : IDekRegistryClient
    {
        private DekRestService restService;

        private int identityMapCapacity;

        private readonly IDictionary<KekId, RegisteredKek> keks = new Dictionary<KekId, RegisteredKek>();

        private readonly IDictionary<DekId, RegisteredDek> deks = new Dictionary<DekId, RegisteredDek>();

        private readonly SemaphoreSlim cacheMutex = new SemaphoreSlim(1);

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
        ///     The default maximum number of connections per server.
        /// </summary>
        public const int DefaultMaxConnectionsPerServer = 20;

        /// <summary>
        ///     The default maximum capacity of the local cache.
        /// </summary>
        public const int DefaultMaxCachedKeys = 1000;

        /// <summary>
        ///     The default SSL server certificate verification for Schema Registry REST API calls.
        /// </summary>
        public const bool DefaultEnableSslCertificateVerification = true;

        /// <inheritdoc />
        public int MaxCachedKeys
            => identityMapCapacity;

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
        public CachedDekRegistryClient(IEnumerable<KeyValuePair<string, string>> config,
            IAuthenticationHeaderValueProvider authenticationHeaderValueProvider,
            IWebProxy proxy = null)
        {
            if (config == null)
            {
                throw new ArgumentNullException("config");
            }
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

            var maxConnectionsPerServerMaybe = config.FirstOrDefault(prop =>
                prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryMaxConnectionsPerServer);
            int maxConnectionsPerServer;
            try
            {
                maxConnectionsPerServer = maxConnectionsPerServerMaybe.Value == null ? DefaultMaxConnectionsPerServer : Convert.ToInt32(maxConnectionsPerServerMaybe.Value);
            }
            catch (FormatException)
            {
                throw new ArgumentException(
                    $"Configured value for {SchemaRegistryConfig.PropertyNames.SchemaRegistryMaxConnectionsPerServer} must be an integer.");
            }

            var identityMapCapacityMaybe = config.FirstOrDefault(prop =>
                prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryMaxCachedSchemas);
            try
            {
                this.identityMapCapacity = identityMapCapacityMaybe.Value == null
                    ? DefaultMaxCachedKeys
                    : Convert.ToInt32(identityMapCapacityMaybe.Value);
            }
            catch (FormatException)
            {
                throw new ArgumentException(
                    $"Configured value for {SchemaRegistryConfig.PropertyNames.SchemaRegistryMaxCachedSchemas} must be an integer.");
            }

            authenticationHeaderValueProvider = DekRestService.AuthenticationHeaderValueProvider(
                config, authenticationHeaderValueProvider, maxRetries, retriesWaitMs, retriesMaxWaitMs);

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
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryMaxConnectionsPerServer &&
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
            var sslCaCertificates = CachedSchemaRegistryClient.LoadCaCertificates(sslCaLocation);
            this.restService = new DekRestService(schemaRegistryUris, timeoutMs, authenticationHeaderValueProvider,
                    SetSslConfig(config), sslVerify, sslCaCertificates, proxy, maxRetries, retriesWaitMs, retriesMaxWaitMs, maxConnectionsPerServer);
        }

        /// <summary>
        ///     Initialize a new instance of the SchemaRegistryClient class.
        /// </summary>
        /// <param name="config">
        ///     Configuration properties.
        /// </param>
        public CachedDekRegistryClient(IEnumerable<KeyValuePair<string, string>> config)
            : this(config, null)
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
            if (keks.Count + deks.Count >= identityMapCapacity)
            {
                this.deks.Clear();
                this.keks.Clear();
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
        public Task<List<string>> GetKeksAsync(bool ignoreDeletedKeks)
            => restService.GetKeksAsync(ignoreDeletedKeks);

        /// <inheritdoc/>
        public async Task<RegisteredKek> GetKekAsync(string name, bool ignoreDeletedKeks)
        {
            await cacheMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
            try
            {
                KekId kekId = new KekId(name, ignoreDeletedKeks);
                if (!this.keks.TryGetValue(kekId, out RegisteredKek kek))
                {
                    CleanCacheIfFull();
                    kek = await restService.GetKekAsync(name, ignoreDeletedKeks)
                        .ConfigureAwait(continueOnCapturedContext: false);
                    this.keks[kekId] = kek;
                }

                return kek;
            }
            finally
            {
                cacheMutex.Release();
            }
        }

        /// <inheritdoc/>
        public Task<RegisteredKek> CreateKekAsync(Kek kek)
            => restService.CreateKekAsync(kek);

        /// <inheritdoc/>
        public Task<RegisteredKek> UpdateKekAsync(string name, UpdateKek kek)
            => restService.UpdateKekAsync(name, kek);

        /// <inheritdoc/>
        public Task<List<string>> GetDeksAsync(string kekName, bool ignoreDeletedDeks)
            => restService.GetDeksAsync(kekName, ignoreDeletedDeks);

        /// <inheritdoc/>
        public Task<List<int>> GetDekVersionsAsync(string kekName, string subject, DekFormat? algorithm,
            bool ignoreDeletedDeks)
            => restService.GetDekVersionsAsync(kekName, subject, algorithm, ignoreDeletedDeks);

        /// <inheritdoc/>
        public async Task<RegisteredDek> GetDekAsync(string kekName, string subject, DekFormat? algorithm,
            bool ignoreDeletedDeks)
        {
            await cacheMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
            try
            {
                DekId dekId = new DekId(kekName, subject, null, algorithm, ignoreDeletedDeks);
                if (!this.deks.TryGetValue(dekId, out RegisteredDek dek))
                {
                    CleanCacheIfFull();
                    dek = await restService.GetDekAsync(kekName, subject, algorithm, ignoreDeletedDeks)
                        .ConfigureAwait(continueOnCapturedContext: false);
                    this.deks[dekId] = dek;
                }

                return dek;
            }
            finally
            {
                cacheMutex.Release();
            }
        }

        /// <inheritdoc/>
        public async Task<RegisteredDek> GetDekVersionAsync(string kekName, string subject, int version, DekFormat? algorithm,
            bool ignoreDeletedDeks)
        {
            await cacheMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
            try
            {
                DekId dekId = new DekId(kekName, subject, version, algorithm, ignoreDeletedDeks);
                if (!this.deks.TryGetValue(dekId, out RegisteredDek dek))
                {
                    CleanCacheIfFull();
                    dek = await restService.GetDekVersionAsync(kekName, subject, version, algorithm, ignoreDeletedDeks)
                        .ConfigureAwait(continueOnCapturedContext: false);
                    this.deks[dekId] = dek;
                }

                return dek;
            }
            finally
            {
                cacheMutex.Release();
            }
        }

        /// <inheritdoc/>
        public Task<RegisteredDek> GetDekLatestVersionAsync(string kekName, string subject, DekFormat? algorithm,
            bool ignoreDeletedDeks)
            => GetDekVersionAsync(kekName, subject, -1, algorithm, ignoreDeletedDeks);

        /// <inheritdoc/>
        public async Task<RegisteredDek> CreateDekAsync(string kekName, Dek dek)
        {
            try
            {
                return await restService.CreateDekAsync(kekName, dek)
                    .ConfigureAwait(continueOnCapturedContext: false);
            }
            finally
            {
                // Ensure latest dek is invalidated, such as in case of conflict (409) or error
                // Invalidate both version=-1 (from GetDekLatestVersionAsync) and version=null (from GetDekAsync)
                await cacheMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
                try
                {
                    this.deks.Remove(new DekId(kekName, dek.Subject, -1, dek.Algorithm, false));
                    this.deks.Remove(new DekId(kekName, dek.Subject, -1, dek.Algorithm, true));
                    this.deks.Remove(new DekId(kekName, dek.Subject, null, dek.Algorithm, false));
                    this.deks.Remove(new DekId(kekName, dek.Subject, null, dek.Algorithm, true));
                }
                finally
                {
                    cacheMutex.Release();
                }
            }
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
