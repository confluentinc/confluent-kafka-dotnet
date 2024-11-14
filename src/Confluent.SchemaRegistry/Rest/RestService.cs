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
using System.Threading.Tasks;
using X509Certificate2 = System.Security.Cryptography.X509Certificates.X509Certificate2;

using System.Security.Cryptography.X509Certificates;
using System.Net.Security;

namespace Confluent.SchemaRegistry
{
    public class RestService : IRestService
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
        public RestService(string schemaRegistryUrl, int timeoutMs,
            IAuthenticationHeaderValueProvider authenticationHeaderValueProvider, List<X509Certificate2> certificates,
            bool enableSslCertificateVerification, X509Certificate2 sslCaCertificate = null)
        {
            this.authenticationHeaderValueProvider = authenticationHeaderValueProvider;

            this.clients = schemaRegistryUrl
                .Split(',')
                .Select(SanitizeUri) // need http or https - use http if not present.
                .Select(uri => new HttpClient(CreateHandler(certificates, enableSslCertificateVerification, sslCaCertificate))
                {
                    BaseAddress = new Uri(uri, UriKind.Absolute), Timeout = TimeSpan.FromMilliseconds(timeoutMs)
                })
                .ToList();
        }

        private static string SanitizeUri(string uri)
        {
            var sanitized = uri.StartsWith("http", StringComparison.Ordinal) ? uri : $"http://{uri}";
            return $"{sanitized.TrimEnd('/')}/";
        }

        private static HttpClientHandler CreateHandler(List<X509Certificate2> certificates,
            bool enableSslCertificateVerification, X509Certificate2 sslCaCertificate)
        {
            var handler = new HttpClientHandler();

            if (!enableSslCertificateVerification)
            {
                handler.ServerCertificateCustomValidationCallback = (_, __, ___, ____) => { return true; };
            } 
            else  if (sslCaCertificate != null)
            {
                handler.ServerCertificateCustomValidationCallback = (_, __, chain, policyErrors) => { 
                    
                    if (policyErrors == SslPolicyErrors.None)
                    {
                        return true;
                    }

                    //The second element of the chain should be the issuer of the certificate
                    if (chain.ChainElements.Count < 2)
                    {
                        return false;
                    }                 
                    var connectionCertHash = chain.ChainElements[1].Certificate.GetCertHash();


                    var expectedCertHash = sslCaCertificate.GetCertHash();

                    if (connectionCertHash.Length != expectedCertHash.Length)
                    {
                        return false;
                    }

                    for (int i = 0; i < connectionCertHash.Length; i++)
                    {
                        if (connectionCertHash[i] != expectedCertHash[i])
                        {
                            return false;
                        }
                    }
                    return true; 
                };
            }

            if (certificates.Count > 0)
            {
                handler.ClientCertificateOptions = ClientCertificateOption.Manual;
                certificates.ForEach(c => handler.ClientCertificates.Add(c));
            }

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
                                await response.Content.ReadAsStringAsync()
                                    .ConfigureAwait(continueOnCapturedContext: false));
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
                            await response.Content.ReadAsStringAsync()
                                .ConfigureAwait(continueOnCapturedContext: false));
                        message = errorObject.Value<string>("message");
                        errorCode = errorObject.Value<int>("error_code");
                    }
                    catch
                    {
                        aggregatedErrorMessage += $"[{clients[clientIndex].BaseAddress}] {response.StatusCode}";
                    }

                    aggregatedErrorMessage +=
                        $"[{clients[clientIndex].BaseAddress}] {response.StatusCode} {errorCode} {message}";
                }
                catch (HttpRequestException e)
                {
                    // don't retry error responses originating from Schema Registry.
                    if (e is SchemaRegistryException)
                    {
                        throw;
                    }

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
        protected async Task<T> RequestAsync<T>(string endPoint, HttpMethod method, params object[] jsonBody)
        {
            var response = await ExecuteOnOneInstanceAsync(() => CreateRequest(endPoint, method, jsonBody))
                .ConfigureAwait(continueOnCapturedContext: false);
            string responseJson =
                await response.Content.ReadAsStringAsync().ConfigureAwait(continueOnCapturedContext: false);
            T t = JObject.Parse(responseJson).ToObject<T>(JsonSerializer.Create());
            return t;
        }

        /// <remarks>
        ///     Used for end points that return a json array [ ... ]
        /// </remarks>
        protected async Task<List<T>> RequestListOfAsync<T>(string endPoint, HttpMethod method, params object[] jsonBody)
        {
            var response = await ExecuteOnOneInstanceAsync(() => CreateRequest(endPoint, method, jsonBody))
                .ConfigureAwait(continueOnCapturedContext: false);
            return JArray.Parse(
                    await response.Content.ReadAsStringAsync().ConfigureAwait(continueOnCapturedContext: false))
                .ToObject<List<T>>(JsonSerializer.Create());
        }

        private HttpRequestMessage CreateRequest(string endPoint, HttpMethod method, params object[] jsonBody)
        {
            HttpRequestMessage request = new HttpRequestMessage(method, endPoint);
            request.Headers.Add("Accept", acceptHeader);
            if (jsonBody.Length != 0)
            {
                string stringContent = string.Join("\n", jsonBody.Select(x => JsonConvert.SerializeObject(x)));
                var content = new StringContent(stringContent, System.Text.Encoding.UTF8,
                    Versions.SchemaRegistry_V1_JSON);
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
            => SanitizeSchema(
                (await RequestAsync<Schema>($"schemas/ids/{id}{(format != null ? "?format=" + format : "")}",
                        HttpMethod.Get)
                    .ConfigureAwait(continueOnCapturedContext: false)));

        public async Task<Schema> GetSchemaBySubjectAndIdAsync(string subject, int id, string format)
            => SanitizeSchema(
                (await RequestAsync<Schema>($"schemas/ids/{id}?subject={(subject ?? "")}{(format != null ? "&format=" + format : "")}",
                        HttpMethod.Get)
                    .ConfigureAwait(continueOnCapturedContext: false)));

        #endregion Schemas

        #region Subjects

        public async Task<List<string>> GetSubjectsAsync()
            => await RequestListOfAsync<string>("subjects", HttpMethod.Get)
                .ConfigureAwait(continueOnCapturedContext: false);

        public async Task<List<int>> GetSubjectVersionsAsync(string subject)
            => await RequestListOfAsync<int>($"subjects/{Uri.EscapeDataString(subject)}/versions", HttpMethod.Get)
                .ConfigureAwait(continueOnCapturedContext: false);

        public async Task<RegisteredSchema> GetSchemaAsync(string subject, int version, bool ignoreDeletedSchemas = true)
            => SanitizeRegisteredSchema(
                await RequestAsync<RegisteredSchema>($"subjects/{Uri.EscapeDataString(subject)}/versions/{version}?deleted={!ignoreDeletedSchemas}",
                        HttpMethod.Get)
                    .ConfigureAwait(continueOnCapturedContext: false));

        public async Task<RegisteredSchema> GetLatestSchemaAsync(string subject)
            => SanitizeRegisteredSchema(
                await RequestAsync<RegisteredSchema>($"subjects/{Uri.EscapeDataString(subject)}/versions/latest",
                        HttpMethod.Get)
                    .ConfigureAwait(continueOnCapturedContext: false));

        public async Task<RegisteredSchema> GetLatestWithMetadataAsync(string subject, IDictionary<string, string> metadata, bool ignoreDeletedSchemas)
            => SanitizeRegisteredSchema(
                await RequestAsync<RegisteredSchema>($"subjects/{Uri.EscapeDataString(subject)}/metadata?{getKeyValuePairs(metadata)}&deleted={!ignoreDeletedSchemas}",
                        HttpMethod.Get)
                    .ConfigureAwait(continueOnCapturedContext: false));
        
        private string getKeyValuePairs(IDictionary<string, string> metadata)
        {
            return string.Join("&", metadata.Select(x => $"key={x.Key}&value={x.Value}"));
        }
        
        public async Task<int> RegisterSchemaAsync(string subject, Schema schema, bool normalize)
            => (await RequestAsync<SchemaId>(
                    $"subjects/{Uri.EscapeDataString(subject)}/versions?normalize={normalize}", HttpMethod.Post,
                    schema)
                .ConfigureAwait(continueOnCapturedContext: false)).Id;

        // Checks whether a schema has been registered under a given subject.
        public async Task<RegisteredSchema> LookupSchemaAsync(string subject, Schema schema, bool ignoreDeletedSchemas,
            bool normalize)
            => await RequestAsync<RegisteredSchema>(
                    $"subjects/{Uri.EscapeDataString(subject)}?normalize={normalize}&deleted={!ignoreDeletedSchemas}",
                    HttpMethod.Post, schema)
                .ConfigureAwait(continueOnCapturedContext: false);

        #endregion Subjects

        #region Compatibility

        public async Task<bool> TestCompatibilityAsync(string subject, int versionId, Schema schema)
            => (await RequestAsync<CompatibilityCheck>(
                    $"compatibility/subjects/{Uri.EscapeDataString(subject)}/versions/{versionId}", HttpMethod.Post,
                    schema)
                .ConfigureAwait(continueOnCapturedContext: false)).IsCompatible;


        public async Task<bool> TestLatestCompatibilityAsync(string subject, Schema schema)
            => (await RequestAsync<CompatibilityCheck>(
                    $"compatibility/subjects/{Uri.EscapeDataString(subject)}/versions/latest", HttpMethod.Post,
                    schema)
                .ConfigureAwait(continueOnCapturedContext: false)).IsCompatible;

        #endregion Compatibility 

        #region Config

        public async Task<Compatibility> UpdateCompatibilityAsync(string subject, Compatibility compatibility)
            => (await RequestAsync<ServerConfig>(
                    string.IsNullOrEmpty(subject) ? "config" : $"config/{Uri.EscapeDataString(subject)}", HttpMethod.Put, 
                    new ServerConfig(compatibility))
                .ConfigureAwait(continueOnCapturedContext: false)).CompatibilityLevel;
        public async Task<Compatibility> GetCompatibilityAsync(string subject)
            => (await RequestAsync<ServerConfig>(
                    string.IsNullOrEmpty(subject) ? "config" : $"config/{Uri.EscapeDataString(subject)}", HttpMethod.Get) 
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