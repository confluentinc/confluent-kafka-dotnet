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

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Security.Cryptography.X509Certificates;

namespace Confluent.SchemaRegistry.Encryption
{
    public class DekRestService : RestService
    {
        /// <summary>
        ///     Initializes a new instance of the RestService class.
        /// </summary>
        public DekRestService(string schemaRegistryUrl, int timeoutMs,
            IAuthenticationHeaderValueProvider authenticationHeaderValueProvider, List<X509Certificate2> certificates,
            bool enableSslCertificateVerification, X509Certificate2 sslCaCertificate = null, IWebProxy proxy = null,
            int maxRetries = DefaultMaxRetries, int retriesWaitMs = DefaultRetriesWaitMs,
            int retriesMaxWaitMs = DefaultRetriesMaxWaitMs) :
            base(schemaRegistryUrl, timeoutMs, authenticationHeaderValueProvider, certificates,
                enableSslCertificateVerification, sslCaCertificate, proxy, maxRetries, retriesWaitMs, retriesMaxWaitMs)
        {
        }

        #region Keks

        public async Task<List<string>> GetKeksAsync(bool ignoreDeletedKeks)
            => await RequestListOfAsync<string>($"dek-registry/v1/keks?deleted={!ignoreDeletedKeks}",
                    HttpMethod.Get)
                .ConfigureAwait(continueOnCapturedContext: false);

        public async Task<RegisteredKek> GetKekAsync(string name, bool ignoreDeletedKeks)
            => await RequestAsync<RegisteredKek>(
                    $"dek-registry/v1/keks/{Uri.EscapeDataString(name)}?deleted={!ignoreDeletedKeks}",
                    HttpMethod.Get)
                .ConfigureAwait(continueOnCapturedContext: false);

        public async Task<RegisteredKek> CreateKekAsync(Kek kek)
            => await RequestAsync<RegisteredKek>($"dek-registry/v1/keks",
                    HttpMethod.Post, kek)
                .ConfigureAwait(continueOnCapturedContext: false);


        public async Task<RegisteredKek> UpdateKekAsync(string name, UpdateKek kek)
            => await RequestAsync<RegisteredKek>($"dek-registry/v1/keks/{Uri.EscapeDataString(name)}",
                    HttpMethod.Put, kek)
                .ConfigureAwait(continueOnCapturedContext: false);

        #endregion Keks

        #region Deks

        public async Task<List<string>> GetDeksAsync(string kekName, bool ignoreDeletedDeks)
            => await RequestListOfAsync<string>(
                    $"dek-registry/v1/keks/{Uri.EscapeDataString(kekName)}/deks?deleted={!ignoreDeletedDeks}",
                    HttpMethod.Get)
                .ConfigureAwait(continueOnCapturedContext: false);

        public async Task<List<int>> GetDekVersionsAsync(string kekName, string subject, DekFormat? algorithm,
            bool ignoreDeletedDeks)
            => await RequestListOfAsync<int>(
                    $"dek-registry/v1/keks/{Uri.EscapeDataString(kekName)}/deks/{Uri.EscapeDataString(subject)}/versions?deleted={!ignoreDeletedDeks}{(algorithm != null ? "&algorithm=" + algorithm : "")}",
                    HttpMethod.Get)
                .ConfigureAwait(continueOnCapturedContext: false);

        public async Task<RegisteredDek> GetDekAsync(string kekName, string subject, DekFormat? algorithm,
            bool ignoreDeletedDeks)
            => await RequestAsync<RegisteredDek>(
                    $"dek-registry/v1/keks/{Uri.EscapeDataString(kekName)}/deks/{Uri.EscapeDataString(subject)}?deleted={!ignoreDeletedDeks}{(algorithm != null ? "&algorithm=" + algorithm : "")}",
                    HttpMethod.Get)
                .ConfigureAwait(continueOnCapturedContext: false);

        public async Task<RegisteredDek> GetDekVersionAsync(string kekName, string subject, int version, DekFormat? algorithm,
            bool ignoreDeletedDeks)
            => await RequestAsync<RegisteredDek>(
                    $"dek-registry/v1/keks/{Uri.EscapeDataString(kekName)}/deks/{Uri.EscapeDataString(subject)}/versions/{version}?deleted={!ignoreDeletedDeks}{(algorithm != null ? "&algorithm=" + algorithm : "")}",
                    HttpMethod.Get)
                .ConfigureAwait(continueOnCapturedContext: false);

        public async Task<RegisteredDek> CreateDekAsync(string kekName, Dek dek)
            => await RequestAsync<RegisteredDek>($"dek-registry/v1/keks/{Uri.EscapeDataString(kekName)}/deks",
                    HttpMethod.Post, dek)
                .ConfigureAwait(continueOnCapturedContext: false);

        #endregion Deks

        protected internal static IAuthenticationHeaderValueProvider
            AuthenticationHeaderValueProvider(
                IEnumerable<KeyValuePair<string, string>> config,
                IAuthenticationHeaderValueProvider authenticationHeaderValueProvider,
                int maxRetries, int retriesWaitMs, int retriesMaxWaitMs)
        {
            return RestService.AuthenticationHeaderValueProvider(config,
                authenticationHeaderValueProvider, maxRetries, retriesWaitMs, retriesMaxWaitMs);
        }
    }
}