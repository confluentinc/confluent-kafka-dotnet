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

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Security.Cryptography.X509Certificates;

namespace Confluent.SchemaRegistry
{
    /// <remarks>
    ///     It may be useful to expose this publicly, but this is not
    ///     required by the Avro serializers, so we will keep this internal
    ///     for now to minimize documentation / risk of API change etc.
    /// </remarks>
    internal class RestService : IRestService
    {
        private readonly List<SchemaReference> EmptyReferencesList = new List<SchemaReference>();

        private static readonly string acceptHeader = string.Join(", ", Versions.PreferredResponseTypes);

        /// <summary>
        ///     The index of the last client successfully used (or random if none worked).
        /// </summary>
        private int lastClientUsed;
        private object lastClientUsedLock = new object();

        /// <summary>
        ///     HttpClient instances corresponding to each provided schema registry Uri.
        /// </summary>
        private readonly List<HttpClient> clients;
        /// <summary>
        ///     HTTP request authentication value provider
        /// </summary>
        private readonly IAuthenticationHeaderValueProvider authenticationHeaderValueProvider;


        /// <summary>
        ///     Initializes a new instance of the RestService class.
        /// </summary>
        public RestService(string schemaRegistryUrl, int timeoutMs, IAuthenticationHeaderValueProvider authenticationHeaderValueProvider, List<X509Certificate2> certificates, bool enableSslCertificateVerification)
        {
            this.authenticationHeaderValueProvider = authenticationHeaderValueProvider;

            this.clients = schemaRegistryUrl
                .Split(',')
                .Select(SanitizeUri)// need http or https - use http if not present.
                .Select(uri =>
                {
                    HttpClient client;
                    if (certificates.Count > 0)
                    {
                        client = new HttpClient(CreateHandler(certificates, enableSslCertificateVerification)) { BaseAddress = new Uri(uri, UriKind.Absolute), Timeout = TimeSpan.FromMilliseconds(timeoutMs) };
                    }
                    else
                    {
                        client = new HttpClient() { BaseAddress = new Uri(uri, UriKind.Absolute), Timeout = TimeSpan.FromMilliseconds(timeoutMs) };
                    }
                    return client;
                })
                .ToList();
        }

        private static string SanitizeUri(string uri)
        {
            var sanitized = uri.StartsWith("http", StringComparison.Ordinal) ? uri : $"http://{uri}";
            return $"{sanitized.TrimEnd('/')}/";
        }

        private static HttpClientHandler CreateHandler(List<X509Certificate2> certificates, bool enableSslCertificateVerification)
        {
            var handler = new HttpClientHandler();
            handler.ClientCertificateOptions = ClientCertificateOption.Manual;

            if (!enableSslCertificateVerification)
            {
                handler.ServerCertificateCustomValidationCallback = (httpRequestMessage, cert, certChain, policyErrors) => { return true; };
            }

            certificates.ForEach(c => handler.ClientCertificates.Add(c));
            return handler;
        }

        private RegisteredSchema SanitizeRegisteredSchema(RegisteredSchema schema)
        {
            if (schema.References == null)
            {
                // The JSON responses from Schema Registry does not include
                // a references list if there are no references, which means
                // schema.References will be null here in that case. It's
                // semantically better if this is an empty list however, so
                // expose that.
                schema.References = EmptyReferencesList;
            }

            return schema;
        }

        private Schema SanitizeSchema(Schema schema)
        {
            if (schema.References == null)
            {
                // The JSON response from Schema Registry does not include
                // a references list if there are no references, which means
                // schema.References will be null here in that case. It's
                // semantically better if this is an empty list however, so
                // expose that.
                schema.References = EmptyReferencesList;
            }

            if (schema.SchemaType_String == null)
            {
                // The JSON response from Schema Registry does not include
                // schemaType in the avro case (only).
                schema.SchemaType = SchemaType.Avro;
            }

            return schema;
        }


        #region Base Requests

        private async Task<HttpResponseMessage> ExecuteOnOneInstanceAsync(Func<HttpRequestMessage> createRequest)
        {
            // There may be many base urls - roll until one is found that works.
            //
            // Start with the last client that was used by this method, which only gets set on 
            // success, so it's probably going to work.
            //
            // Otherwise, try every client until a successful call is made (true even under 
            // concurrent access).

            string aggregatedErrorMessage = null;
            HttpResponseMessage response = null;
            bool firstError = true;

            int startClientIndex;
            lock (lastClientUsedLock)
            {
                startClientIndex = this.lastClientUsed;
            }

            int loopIndex = startClientIndex;
            int clientIndex = -1; // prevent uninitialized variable compiler error.
            bool finished = false;
            for (; loopIndex < clients.Count + startClientIndex && !finished; ++loopIndex)
            {
                clientIndex = loopIndex % clients.Count;

                try
                {
                    response = await clients[clientIndex]
                            .SendAsync(createRequest())
                            .ConfigureAwait(continueOnCapturedContext: false);

                    if (response.StatusCode == HttpStatusCode.OK ||
                        response.StatusCode == HttpStatusCode.NoContent)
                    {
                        lock (lastClientUsedLock)
                        {
                            this.lastClientUsed = clientIndex;
                        }

                        return response;
                    }

                    string message = "";
                    int errorCode = -1;

                    // 4xx errors with valid SR error message as content should not be retried (these are conclusive).
                    if ((int)response.StatusCode >= 400 && (int)response.StatusCode < 500)
                    {
                        try
                        {
                            JObject errorObject = null;
                            errorObject = JObject.Parse(
                                await response.Content.ReadAsStringAsync().ConfigureAwait(continueOnCapturedContext: false));
                            message = errorObject.Value<string>("message");
                            errorCode = errorObject.Value<int>("error_code");
                        }
                        catch (Exception)
                        {
                            // consider an unauthorized response from any server to be conclusive.
                            if (response.StatusCode == HttpStatusCode.Unauthorized)
                            {
                                finished = true;
                                throw new HttpRequestException($"Unauthorized");
                            }
                        }
                        throw new SchemaRegistryException(message, response.StatusCode, errorCode);
                    }

                    if (!firstError)
                    {
                        aggregatedErrorMessage += "; ";
                    }
                    firstError = false;

                    try
                    {
                        var errorObject = JObject.Parse(
                            await response.Content.ReadAsStringAsync().ConfigureAwait(continueOnCapturedContext: false));
                        message = errorObject.Value<string>("message");
                        errorCode = errorObject.Value<int>("error_code");
                    }
                    catch
                    {
                        aggregatedErrorMessage += $"[{clients[clientIndex].BaseAddress}] {response.StatusCode}";
                    }

                    aggregatedErrorMessage += $"[{clients[clientIndex].BaseAddress}] {response.StatusCode} {errorCode} {message}";
                }
                catch (HttpRequestException e)
                {
                    // don't retry error responses originating from Schema Registry.
                    if (e is SchemaRegistryException) { throw; }

                    if (!firstError)
                    {
                        aggregatedErrorMessage += "; ";
                    }
                    firstError = false;

                    aggregatedErrorMessage += $"[{clients[clientIndex].BaseAddress}] HttpRequestException: {e.Message}";
                }
            }

            throw new HttpRequestException(aggregatedErrorMessage);
        }


        /// <remarks>
        ///     Used for end points that return a json object { ... }
        /// </remarks>
        private async Task<T> RequestAsync<T>(string endPoint, HttpMethod method, params object[] jsonBody)
        {
            var response = await ExecuteOnOneInstanceAsync(() => CreateRequest(endPoint, method, jsonBody)).ConfigureAwait(continueOnCapturedContext: false);
            string responseJson = await response.Content.ReadAsStringAsync().ConfigureAwait(continueOnCapturedContext: false);
            T t = JObject.Parse(responseJson).ToObject<T>();
            return t;
        }

        /// <remarks>
        ///     Used for end points that return a json array [ ... ]
        /// </remarks>
        private async Task<List<T>> RequestListOfAsync<T>(string endPoint, HttpMethod method, params object[] jsonBody)
        {
            var response = await ExecuteOnOneInstanceAsync(() => CreateRequest(endPoint, method, jsonBody))
                                    .ConfigureAwait(continueOnCapturedContext: false);
            return JArray.Parse(
                await response.Content.ReadAsStringAsync().ConfigureAwait(continueOnCapturedContext: false)).ToObject<List<T>>();
        }

        private HttpRequestMessage CreateRequest(string endPoint, HttpMethod method, params object[] jsonBody)
        {
            HttpRequestMessage request = new HttpRequestMessage(method, endPoint);
            request.Headers.Add("Accept", acceptHeader);
            if (jsonBody.Length != 0)
            {
                string stringContent = string.Join("\n", jsonBody.Select(x => JsonConvert.SerializeObject(x)));
                var content = new StringContent(stringContent, System.Text.Encoding.UTF8, Versions.SchemaRegistry_V1_JSON);
                content.Headers.ContentType.CharSet = string.Empty;
                request.Content = content;
            }
            if (authenticationHeaderValueProvider != null)
            {
                request.Headers.Authorization = authenticationHeaderValueProvider.GetAuthenticationHeader();
            }
            return request;
        }

        #endregion Base Requests

        #region Schemas

        public async Task<Schema> GetSchemaAsync(int id, string format)
            => SanitizeSchema((await RequestAsync<Schema>($"schemas/ids/{id}{(format != null ? "?format=" + format : "")}", HttpMethod.Get)
                        .ConfigureAwait(continueOnCapturedContext: false)));

        #endregion Schemas

        #region Subjects

        public async Task<List<string>> GetSubjectsAsync()
            => await RequestListOfAsync<string>("subjects", HttpMethod.Get)
                        .ConfigureAwait(continueOnCapturedContext: false);

        public async Task<List<int>> GetSubjectVersionsAsync(string subject)
            => await RequestListOfAsync<int>($"subjects/{WebUtility.UrlEncode(subject)}/versions", HttpMethod.Get)
                        .ConfigureAwait(continueOnCapturedContext: false);

        public async Task<RegisteredSchema> GetSchemaAsync(string subject, int version)
            => SanitizeRegisteredSchema(await RequestAsync<RegisteredSchema>($"subjects/{WebUtility.UrlEncode(subject)}/versions/{version}", HttpMethod.Get)
                        .ConfigureAwait(continueOnCapturedContext: false));

        public async Task<RegisteredSchema> GetLatestSchemaAsync(string subject)
            => SanitizeRegisteredSchema(await RequestAsync<RegisteredSchema>($"subjects/{WebUtility.UrlEncode(subject)}/versions/latest", HttpMethod.Get)
                        .ConfigureAwait(continueOnCapturedContext: false));

        public async Task<int> RegisterSchemaAsync(string subject, Schema schema, bool normalize)
            => schema.SchemaType == SchemaType.Avro
                // In the avro case, just send the schema string to maintain backards compatibility.
                ? (await RequestAsync<SchemaId>($"subjects/{WebUtility.UrlEncode(subject)}/versions?normalize={normalize}", HttpMethod.Post, new SchemaString(schema.SchemaString))
                        .ConfigureAwait(continueOnCapturedContext: false)).Id
                : (await RequestAsync<SchemaId>($"subjects/{WebUtility.UrlEncode(subject)}/versions?normalize={normalize}", HttpMethod.Post, schema)
                        .ConfigureAwait(continueOnCapturedContext: false)).Id;

        // Checks whether a schema has been registered under a given subject.
        public async Task<RegisteredSchema> LookupSchemaAsync(string subject, Schema schema, bool ignoreDeletedSchemas, bool normalize)
            => SanitizeRegisteredSchema(schema.SchemaType == SchemaType.Avro
                // In the avro case, just send the schema string to maintain backards compatibility.
                ? await RequestAsync<RegisteredSchema>($"subjects/{WebUtility.UrlEncode(subject)}?normalize={normalize}&deleted={!ignoreDeletedSchemas}", HttpMethod.Post, new SchemaString(schema.SchemaString))
                        .ConfigureAwait(continueOnCapturedContext: false)
                : await RequestAsync<RegisteredSchema>($"subjects/{WebUtility.UrlEncode(subject)}?normalize={normalize}&deleted={!ignoreDeletedSchemas}", HttpMethod.Post, schema)
                        .ConfigureAwait(continueOnCapturedContext: false));

        #endregion Subjects

        #region Compatibility

        public async Task<bool> TestCompatibilityAsync(string subject, int versionId, Schema schema)
            => schema.SchemaType == SchemaType.Avro
                // In the avro case, just send the schema string to maintain backards compatibility.
                ? (await RequestAsync<CompatibilityCheck>($"compatibility/subjects/{WebUtility.UrlEncode(subject)}/versions/{versionId}", HttpMethod.Post, new SchemaString(schema.SchemaString))
                        .ConfigureAwait(continueOnCapturedContext: false)).IsCompatible
                : (await RequestAsync<CompatibilityCheck>($"compatibility/subjects/{WebUtility.UrlEncode(subject)}/versions/{versionId}", HttpMethod.Post, schema)
                        .ConfigureAwait(continueOnCapturedContext: false)).IsCompatible;


        public async Task<bool> TestLatestCompatibilityAsync(string subject, Schema schema)
            => schema.SchemaType == SchemaType.Avro
                // In the avro case, just send the schema string to maintain backards compatibility.
                ? (await RequestAsync<CompatibilityCheck>($"compatibility/subjects/{WebUtility.UrlEncode(subject)}/versions/latest", HttpMethod.Post, new SchemaString(schema.SchemaString))
                        .ConfigureAwait(continueOnCapturedContext: false)).IsCompatible
                : (await RequestAsync<CompatibilityCheck>($"compatibility/subjects/{WebUtility.UrlEncode(subject)}/versions/latest", HttpMethod.Post, schema)
                        .ConfigureAwait(continueOnCapturedContext: false)).IsCompatible;

        #endregion Compatibility 

        #region Config

        public async Task<Compatibility> GetCompatibilityAsync(string subject)
            => (await RequestAsync<Config>(
                    string.IsNullOrEmpty(subject) ? "config" : $"config/{WebUtility.UrlEncode(subject)}",
                    HttpMethod.Get)
                .ConfigureAwait(continueOnCapturedContext: false)).CompatibilityLevel;

        public async Task<Compatibility> UpdateCompatibilityAsync(string subject, Compatibility compatibility)
            => (await RequestAsync<Config>(
                    string.IsNullOrEmpty(subject) ? "config" : $"config/{WebUtility.UrlEncode(subject)}",
                    HttpMethod.Put, new Config(compatibility))
                .ConfigureAwait(continueOnCapturedContext: false)).CompatibilityLevel;

        #endregion Config

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                foreach (var client in this.clients)
                {
                    client.Dispose();
                }
            }
        }

    }
}
