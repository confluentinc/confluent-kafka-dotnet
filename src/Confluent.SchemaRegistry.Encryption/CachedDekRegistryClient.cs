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
using System.ComponentModel;
using System.Threading;
using System.Security.Cryptography.X509Certificates;

namespace Confluent.SchemaRegistry.Encryption
{
    public record KekId(string Name, bool LookupDeletedKeks);
    
    public record DekId(string KekName, string Subject, int? Version, DekFormat? DekFormat, bool LookupDeletedDeks);
    
    /// <summary>
    ///     A caching DEK Registry client.
    /// </summary>
    public class CachedDekRegistryClient : IDekRegistryClient, IDisposable
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
        public CachedDekRegistryClient(IEnumerable<KeyValuePair<string, string>> config,
            IAuthenticationHeaderValueProvider authenticationHeaderValueProvider)
        {
            if (config == null)
            {
                throw new ArgumentNullException("config properties must be specified.");
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
            }
            else
            {
                throw new ArgumentException(
                    $"Invalid value '{basicAuthSource}' specified for property '{SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthCredentialsSource}'");
            }

            if (authenticationHeaderValueProvider != null)
            {
                if (username != null || password != null)
                {
                    throw new ArgumentException(
                        $"Invalid authentication header value provider configuration: Cannot specify both custom provider and username/password");
                }
            }
            else
            {
                if (username != null && password == null)
                {
                    throw new ArgumentException(
                        $"Invalid authentication header value provider configuration: Basic authentication username specified, but password not specified");
                }

                if (username == null && password != null)
                {
                    throw new ArgumentException(
                        $"Invalid authentication header value provider configuration: Basic authentication password specified, but username not specified");
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
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryLatestCacheTtlSecs &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthCredentialsSource &&
                    property.Key != SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthUserInfo &&
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

            this.restService = new DekRestService(schemaRegistryUris, timeoutMs, authenticationHeaderValueProvider,
                SetSslConfig(config), sslVerify);
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

            var caLocation =
                config.FirstOrDefault(prop => prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SslCaLocation)
                    .Value ?? "";
            if (!String.IsNullOrEmpty(caLocation))
            {
                certificates.Add(new X509Certificate2(caLocation));
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
        public Task<RegisteredDek> CreateDekAsync(string kekName, Dek dek)
        {
            try
            {
                return restService.CreateDekAsync(kekName, dek);
            }
            finally
            {
                // Ensure latest dek is invalidated, such as in case of conflict (409)
                this.deks.Remove(new DekId(kekName, dek.Subject, -1, dek.Algorithm, false));
                this.deks.Remove(new DekId(kekName, dek.Subject, -1, dek.Algorithm, true));
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