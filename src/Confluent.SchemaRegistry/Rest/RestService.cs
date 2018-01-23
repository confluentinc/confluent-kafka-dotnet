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
using System.Net;
using System.Linq;
using System.Net.Http;
using System;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;


namespace Confluent.SchemaRegistry
{
    /// <remarks>
    ///     It may be useful to expose this publically, but this is not
    ///     required by the Avro serializers, so we will keep this internal 
    ///     for now to minimize documentation / risk of API change etc.
    /// </remarks>
    internal class RestService : IRestService
    {
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
        ///     Initializes a new instance of the RestService class.
        /// </summary>
        public RestService(string schemaRegistryUrl, int timeoutMs)
        { 
            this.clients = schemaRegistryUrl
                .Split(',')
                .Select(uri => uri.StartsWith("http", StringComparison.Ordinal) ? uri : "http://" + uri) // need http or https - use http if not present.
                .Select(uri => new HttpClient() { BaseAddress = new Uri(uri, UriKind.Absolute), Timeout = TimeSpan.FromMilliseconds(timeoutMs) })
                .ToList();
        }

        #region Base Requests

        private async Task<HttpResponseMessage> ExecuteOnOneInstanceAsync(HttpRequestMessage request)
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
            for (; loopIndex < clients.Count + startClientIndex; ++loopIndex)
            {
                clientIndex = loopIndex % clients.Count;

                try
                {
                    response = await clients[clientIndex]
                            .SendAsync(request).ConfigureAwait(false);

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
                        var errorObject = JObject.Parse(await response.Content.ReadAsStringAsync().ConfigureAwait(false));
                        message = errorObject.Value<string>("message");
                        errorCode = errorObject.Value<int>("error_code");
                        throw new SchemaRegistryException(message, response.StatusCode, errorCode);
                    }

                    if (!firstError)
                    {
                        aggregatedErrorMessage += "; ";
                    }
                    firstError = false;

                    try
                    {
                        var errorObject = JObject.Parse(await response.Content.ReadAsStringAsync().ConfigureAwait(false));
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
        ///     Used for end points that return return a json object { ... }
        /// </remarks>
        private async Task<T> RequestAsync<T>(string endPoint, HttpMethod method, params object[] jsonBody)
        {
            var request = CreateRequest(endPoint, method, jsonBody);
            var response = await ExecuteOnOneInstanceAsync(request).ConfigureAwait(false);
            string responseJson = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            T t = JObject.Parse(responseJson).ToObject<T>();
            return t;
        }

        /// <remarks>
        ///     Used for end points that return a json array [ ... ]
        /// </remarks>
        private async Task<List<T>> RequestListOfAsync<T>(string endPoint, HttpMethod method, params object[] jsonBody)
        {
            var request = CreateRequest(endPoint, method, jsonBody);
            var response = await ExecuteOnOneInstanceAsync(request).ConfigureAwait(false);
            return JArray.Parse(await response.Content.ReadAsStringAsync().ConfigureAwait(false)).ToObject<List<T>>();
        }

        private HttpRequestMessage CreateRequest(string endPoint, HttpMethod method, params object[] jsonBody)
        {
            HttpRequestMessage request = new HttpRequestMessage(method, endPoint);
            request.Headers.Add("Accept", acceptHeader);
            if (jsonBody.Length != 0)
            {
                string stringContent = string.Join("\n", jsonBody.Select(x => JsonConvert.SerializeObject(x)));
                request.Content = new StringContent(stringContent, System.Text.Encoding.UTF8, Versions.SchemaRegistry_V1_JSON);
            }
            return request;
        }

        #endregion Base Requests

        #region Schemas

        public async Task<string> GetSchemaAsync(int id)
            => (await RequestAsync<SchemaString>($"/schemas/ids/{id}", HttpMethod.Get)).Schema;

        #endregion Schemas

        #region Subjects

        public Task<List<string>> GetSubjectsAsync()
            => RequestListOfAsync<string>(
                    "/subjects", 
                    HttpMethod.Get);

        public Task<List<string>> GetSubjectVersionsAsync(string subject)
            => RequestListOfAsync<string>(
                    $"/subjects/{subject}/versions", 
                    HttpMethod.Get);

        public Task<Schema> GetSchemaAsync(string subject, int version)
            => RequestAsync<Schema>(
                    $"/subjects/{subject}/versions/{version}", 
                    HttpMethod.Get);

        public Task<Schema> GetLatestSchemaAsync(string subject)
            => RequestAsync<Schema>(
                    $"/subjects/{subject}/versions/latest", 
                    HttpMethod.Get);

        public async Task<int> RegisterSchemaAsync(string subject, string schema)
            => (await RequestAsync<SchemaId>(
                    $"/subjects/{subject}/versions", 
                    HttpMethod.Post, 
                    new SchemaString(schema))).Id;

        public Task<Schema> CheckSchemaAsync(string subject, string schema, bool ignoreDeletedSchemas)
            => RequestAsync<Schema>(
                    $"/subjects/{subject}?deleted={!ignoreDeletedSchemas}",
                    HttpMethod.Post, 
                    new SchemaString(schema));

        public Task<Schema> CheckSchemaAsync(string subject, string schema)
            => RequestAsync<Schema>(
                    $"/subjects/{subject}", 
                    HttpMethod.Post, 
                    new SchemaString(schema));

        #endregion Subjects

        #region Compatibility

        public async Task<bool> TestCompatibilityAsync(string subject, int versionId, string schema)
            => (await RequestAsync<CompatibilityCheck>(
                    $"/compatibility/subjects/{subject}/versions/{versionId}",
                    HttpMethod.Post,
                    new SchemaString(schema))).IsCompatible;

        public async Task<bool> TestLatestCompatibilityAsync(string subject, string schema)
            => (await RequestAsync<CompatibilityCheck>(
                    $"/compatibility/subjects/{subject}/versions/latest",
                    HttpMethod.Post,
                    new SchemaString(schema))).IsCompatible;

        #endregion Compatibility

        #region Config

        public async Task<Compatibility> GetGlobalCompatibilityAsync()
            => (await RequestAsync<Config>(
                    "/config", 
                    HttpMethod.Get)).CompatibilityLevel;

        public async Task<Compatibility> GetCompatibilityAsync(string subject)
            => (await RequestAsync<Config>(
                    $"/config/{subject}", 
                    HttpMethod.Get)).CompatibilityLevel;

        public Task<Config> SetGlobalCompatibilityAsync(Compatibility compatibility)
            => RequestAsync<Config>(
                    "/config", 
                    HttpMethod.Put, 
                    new Config(compatibility));

        public Task<Config> SetCompatibilityAsync(string subject, Compatibility compatibility)
            => RequestAsync<Config>(
                    $"/config/{subject}", 
                    HttpMethod.Put, 
                    new Config(compatibility));
            
        #endregion Config

        public void Dispose()
        {
            foreach (var client in this.clients)
            {
                client.Dispose();
            }    
        }
    }
}
