// Copyright 2016-2017 Confluent Inc.
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
using System.IO;
using System;
using System.Threading.Tasks;
using Confluent.Kafka.SchemaRegistry.Rest.Entities.Requests;
using Confluent.Kafka.SchemaRegistry.Rest.Entities;
using Confluent.Kafka.SchemaRegistry.Rest.Exceptions;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;


namespace Confluent.Kafka.SchemaRegistry.Rest
{
    /// <summary>
    ///     Communique avec l'api REST de schema registry pour enregistrer/récupérer les schema AVRO
    /// </summary>
    public class SchemaRegistryRestService : ISchemaRegistyRestService
    {
        public const int DefaultTimetout = 2000;

        private static readonly string _acceptHeader = string.Join(", ", Versions.PREFERRED_RESPONSE_TYPES);

        private int _lastClientUsed; //Last client successfully used (or random if none worked)
        private readonly List<HttpClient> _clients;


        public SchemaRegistryRestService(List<HttpClient> clients)
        {
            _clients = clients;
        }
        
        /// <summary>
        /// </summary>
        /// <param name="schemaRegistryUris">
        ///     Adresses vers les instances, séparées par une virgule. si vide, aucune requete ne sera executée et tous les appels lanceront des exceptions
        /// </param>
        /// <param name="timeoutMs">
        /// </param>
        /// <exception cref="UriFormatException">
        ///     une des uri n'est pas valable
        /// </exception>
        public SchemaRegistryRestService(string schemaRegistryUris, int timeoutMs = DefaultTimetout)
            : this(ExtractRestClientList(schemaRegistryUris, timeoutMs))
        { }

        static List<HttpClient> ExtractRestClientList(string schemaRegistryUris, int timeoutMs)
        {
            return ExtractRestClientList(schemaRegistryUris?.Split(','), timeoutMs);
        }

        static List<HttpClient> ExtractRestClientList(IEnumerable<string> schemaRegistryUris, int timeoutMs)
        {
            return schemaRegistryUris
                .Select(uri => uri.StartsWith("http", StringComparison.Ordinal) ? uri : "http://" + uri) //need http or https - http if not present
                .Select(uri => new HttpClient() { BaseAddress = new Uri(uri, UriKind.Absolute), Timeout = TimeSpan.FromMilliseconds(timeoutMs) })
                .ToList();
        }

        #region Base Requests
        /// <summary>
        ///
        /// </summary>
        /// <param name="request">
        /// </param>
        /// <exception cref="IOException">
        ///     All instances returned HttpRequestException
        /// </exception>
        /// <exception cref="SchemaRegistryInternalException">
        ///     Schema registry server returned exception
        /// </exception>
        /// <returns>
        /// </returns>
        private async Task<HttpResponseMessage> ExecuteOnOneInstanceAsync(HttpRequestMessage request)
        {
            //TODO not thread safe with _lastClientUsed?

            string aggregatedErrorMessage = null;
            HttpResponseMessage response = null;
            bool gotOkAnswer = false;
            for (int i = 0; i < _clients.Count && !gotOkAnswer; i++)
            {
                gotOkAnswer = true;
                //We have many base urls, we roll until we find a correct one
                try
                {
                    response = await _clients[_lastClientUsed].SendAsync(request).ConfigureAwait(false);
                }
                catch (HttpRequestException e)
                {
                    aggregatedErrorMessage += $"{_clients[i].BaseAddress} : {e.Message}";
                    //use next url
                    _lastClientUsed = (_lastClientUsed + 1) % _clients.Count;
                    gotOkAnswer = false;
                }
            }
            if (!gotOkAnswer)
            {
                //All schemas failed, return exception for each one
                throw new IOException(aggregatedErrorMessage);
            }
            if (response.StatusCode != HttpStatusCode.OK)
            {
                //We may have contacted a schema registry whith a default (disconnected from the cluster...),
                //by precaution we roll for a next one
                _lastClientUsed = (_lastClientUsed + 1) % _clients.Count;
                var errorObject = JObject.Parse(await response.Content.ReadAsStringAsync().ConfigureAwait(false));
                throw new SchemaRegistryInternalException(errorObject.Value<string>("message"), response.StatusCode, errorObject.Value<int>("error_code"));
            }
            return response;
        }


        /// <summary>
        ///     Used when endPoint return a json object { ... }
        /// </summary>
        private async Task<T> RequestToAsync<T>(string endPoint, HttpMethod method, params object[] jsonBody)
        {
            var request = CreateRequest(endPoint, method, jsonBody);
            var response = await ExecuteOnOneInstanceAsync(request).ConfigureAwait(false);
            string responseJson = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            T t = JObject.Parse(responseJson).ToObject<T>();
            return t;
        }

        /// <summary>
        ///     Used when endPoint return a json array [ ... ]
        /// </summary>
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="endPoint">
        /// </param>
        /// <param name="method">
        /// </param>
        /// <param name="jsonBody">
        /// </param>
        /// <returns>
        /// </returns>
        private async Task<List<T>> RequestToListOfAsync<T>(string endPoint, HttpMethod method, params object[] jsonBody)
        {
            var request = CreateRequest(endPoint, method, jsonBody);
            var response = await ExecuteOnOneInstanceAsync(request).ConfigureAwait(false);

            return JArray.Parse(await response.Content.ReadAsStringAsync().ConfigureAwait(false)).ToObject<List<T>>();
        }

        private HttpRequestMessage CreateRequest(string endPoint, HttpMethod method, params object[] jsonBody)
        {
            HttpRequestMessage request = new HttpRequestMessage(method, endPoint);
            request.Headers.Add("Accept", _acceptHeader);
            if (jsonBody.Length != 0)
            {
                string stringContent = string.Join("\n", jsonBody.Select(x => JsonConvert.SerializeObject(x)));
                request.Content = new StringContent(stringContent, System.Text.Encoding.UTF8, Versions.SCHEMA_REGISTRY_V1_JSON);
            }

            return request;
        }
        #endregion Base Requests

        #region Schemas
        // The subjects resource provides a list of all registered subjects in your schema registry.
        // A subject refers to the name under which the schema is registered.
        // If you are using the schema registry for Kafka,
        // then a subject refers to either a “<topic>-key” or “<topic>-value”
        // depending on whether you are registering the key schema for that topic or the value schema.

        /// <summary>
        ///     Get the schema string identified by the input id.
        /// </summary>
        /// <param name="id">
        ///     the globally unique identifier of the schema
        /// </param>
        /// <returns>
        ///     Schema string identified by the id
        /// </returns>
        /// <exception cref="IOException">
        ///     Aucune instance n'a pu exécuter la requête
        /// </exception>
        /// <exception cref="SchemaRegistryInternalException">
        ///     Error code 40403 – Schema not found
        ///     Error code 50001 – Error in the backend datastore
        /// </exception>
        public Task<SchemaString> GetSchemaAsync(int id)
            => RequestToAsync<SchemaString>($"/schemas/ids/{id}", HttpMethod.Get);
        #endregion Schemas

        #region Subjects
        /// <summary>
        ///     Get a list of registered subjects.
        /// </summary>
        /// <returns>
        ///     List of subjects
        /// </returns>
        /// <exception cref="IOException">
        ///     Aucune instance n'a pu exécuter la requête
        /// </exception>
        /// <exception cref="SchemaRegistryInternalException">
        ///     Error code 50001 – Error in the backend datastore
        /// </exception>
        public Task<List<string>> GetSubjectsAsync()
            => RequestToListOfAsync<string>("/subjects", HttpMethod.Get);

        /// <summary>
        ///     Get a list of versions registered under the specified subject.
        /// </summary>
        /// <param name="subject">
        ///     the name of the subject
        /// </param>
        /// <returns>
        ///     List of versions of the schema registered under this subject
        /// </returns>
        /// <exception cref="IOException">
        ///     Aucune instance n'a pu exécuter la requête
        /// </exception>
        /// <exception cref="SchemaRegistryInternalException">
        ///     Error code 40401 – Subject not found
        ///     Error code 50001 – Error in the backend datastore
        /// </exception>
        public Task<List<string>> GetSubjectVersions(string subject)
        {
            return RequestToListOfAsync<string>($"/subjects/{subject}/versions", HttpMethod.Get);
        }

        /// <summary>
        ///     Get a specific version of the schema registered under this subject.
        /// </summary>
        /// <param name="subject">
        ///     Name of the subject
        /// </param>
        /// <param name="version">
        ///     Version of the schema to be returned. Valid values for version are between [1,2^31-1]
        /// </param>
        /// <returns>
        ///     Corresponding Schema
        /// </returns>
        /// <exception cref="IOException">
        ///     Aucune instance n'a pu exécuter la requête
        /// </exception>
        /// <exception cref="SchemaRegistryInternalException">
        ///     Error code 40401 – Subject not found
        ///     Error code 40402 – Version not found
        ///     Error code 42202 – Invalid version
        ///     Error code 50001 – Error in the backend data store
        /// </exception>
        public Task<Schema> GetSchemaAsync(string subject, int version)
        {
            return RequestToAsync<Schema>($"/subjects/{subject}/versions/{version}", HttpMethod.Get);
        }

        /// <summary>
        ///     Get the last registered schema under the specified subject
        ///     Note that there may be a new latest schema that gets registered right after this request
        /// </summary>
        /// <param name="subject">
        ///     Name of the subject
        /// </param>
        /// <returns>
        ///     Corresponding Schema
        /// </returns>
        /// <exception cref="IOException">
        ///     Aucune instance n'a pu exécuter la requête
        /// </exception>
        /// <exception cref="SchemaRegistryInternalException">
        ///     Error code 40401 – Subject not found
        ///     Error code 50001 – Error in the backend data store
        /// </exception>
        public Task<Schema> GetLatestSchemaAsync(string subject)
        {
            return RequestToAsync<Schema>($"/subjects/{subject}/versions/latest", HttpMethod.Get);
        }

        /// <summary>
        ///     Register a new schema under the specified subject.
        ///     If successfully registered, this returns the unique identifier of this schema in the registry.
        ///     The returned identifier should be used to retrieve this schema from the schemas resource
        ///     and is different from the schema’s version which is associated with the subject.
        ///     If the same schema is registered under a different subject, the same identifier will be returned.
        ///     However, the version of the schema may be different under different subjects.
        ///
        ///     A schema should be compatible with the previously registered schemas(if there are any)
        ///     as per the configured compatibility level.
        ///     The configured compatibility level can be obtained by issuing by <see cref="GetCompatibilityAsync(string)"/>.
        ///     If that returns null, then <see cref="GetGlobalCompatibilityAsync()"/>
        ///
        ///     When there are multiple instances of schema registry running in the same cluster,
        ///     the schema registration request will be forwarded to one of the instances designated as the master.
        ///     If the master is not available, the client will get an error code indicating that the forwarding has failed.
        /// </summary>
        /// <param name="subject">
        ///     Subject under which the schema will be registered
        /// </param>
        /// <param name="schema">
        ///     The Avro schema string
        /// </param>
        /// <returns>
        ///     Globally unique identifier of the schema
        /// </returns>
        /// <exception cref="IOException">
        ///     Aucune instance n'a pu exécuter la requête
        /// </exception>
        /// <exception cref="SchemaRegistryInternalException">
        ///     409 Conflict – Incompatible Avro schema
        ///     Error code 42201 – Invalid Avro schema
        ///     Error code 50001 – Error in the backend data store
        ///     Error code 50002 – Operation timed out
        ///     Error code 50003 – Error while forwarding the request to the master
        /// </exception>
        public Task<SchemaId> PostSchemaAsync(string subject, string schema)
        {
            return RequestToAsync<SchemaId>($"/subjects/{subject}/versions", HttpMethod.Post, new SchemaString(schema));
        }

        /// <summary>
        ///     Check if a schema has already been registered under the specified subject.
        ///     If so, this returns the schema string along with its globally unique identifier,
        ///     its version under this subject and the subject name.
        /// </summary>
        /// <param name="subject">
        ///     Subject under which the schema will be registered
        /// </param>
        /// <param name="schema">
        ///     The Avro schema string
        /// </param>
        /// <returns>
        /// </returns>
        public Task<Schema> CheckSchemaAsync(string subject, string schema)
        {
            return RequestToAsync<Schema>($"/subjects/{subject}", HttpMethod.Post, new SchemaString(schema));
        }
        #endregion Subjects

        #region Compatibility

        // The compatibility resource allows the user to test schemas
        // for compatibility against specific versions of a subject’s schema.

        /// <summary>
        ///     Test input schema against a particular version of a subject’s schema for 
        ///     compatibility. Note that the compatibility level applied for the check is 
        ///     the configured compatibility level for the subject 
        ///     (<see cref="GetCompatibilityAsync(string)"/>). If this subject’s compatibility
        ///     level was never changed, then the global compatibility level applies 
        ///     (<see cref="GetCompatibilityAsync"/>) .
        /// </summary>
        /// <param name="subject">
        ///     Subject of the schema version against which compatibility is to be tested
        /// </param>
        /// <param name="versionId">
        ///     Version of the subject’s schema against which compatibility is to be tested
        /// </param>
        /// <param name="avroSchema">
        ///     Le schema qu'on souhaite comparer
        /// </param>
        /// <returns>
        /// </returns>
        /// <exception cref="IOException">
        ///     Aucune instance n'a pu exécuter la requête
        /// </exception>
        /// <exception cref="SchemaRegistryInternalException">
        ///     Error code 40401 – Subject not found
        ///     Error code 40402 – Version not found
        ///     Error code 42201 – Invalid Avro schema
        ///     Error code 42202 – Invalid version
        ///     Error code 50001 – Error in the backend data store
        /// </exception>
        public Task<CompatibilityCheckResponse> TestCompatibilityAsync(string subject, int versionId, string avroSchema)
        {
            return RequestToAsync<CompatibilityCheckResponse>(
                $"/compatibility/subjects/{subject}/versions/{versionId}",
                HttpMethod.Post,
                new SchemaString(avroSchema));
        }


        /// <summary>
        ///     Test input schema against latest version of a subject’s schema for compatibility.
        ///     Note that the compatibility level applied for the check is the configured compatibility level for the subject (<see cref="GetCompatibilityAsync(string)"/>).
        ///     If this subject’s compatibility level was never changed, then the global compatibility level applies (<see cref="GetCompatibilityAsync"/>) .
        /// </summary>
        /// <param name="subject">
        ///     Subject of the schema version against which compatibility is to be tested
        /// </param>
        /// <param name="avroSchema">
        ///     Le schema qu'on souhaite comparer
        /// </param>
        /// <returns>
        /// </returns>
        /// <exception cref="IOException">
        ///     Aucune instance n'a pu exécuter la requête
        /// </exception>
        /// <exception cref="SchemaRegistryInternalException">
        ///     Error code 40401 – Subject not found
        ///     Error code 40402 – Version not found
        ///     Error code 42201 – Invalid Avro schema
        ///     Error code 42202 – Invalid version
        ///     Error code 50001 – Error in the backend data store
        /// </exception>
        public Task<CompatibilityCheckResponse> TestLatestCompatibilityAsync(string subject, string avroSchema)
        {
            return RequestToAsync<CompatibilityCheckResponse>(
                $"/compatibility/subjects/{subject}/versions/latest",
                HttpMethod.Post,
                new SchemaString(avroSchema));
        }

        #endregion Compatibility

        #region Config

        //The config resource allows you to inspect the cluster-level configuration values as well as subject overrides.

        /// <summary>
        /// </summary>
        /// <exception cref="IOException">
        ///     Aucune instance n'a pu exécuter la requête
        /// </exception>
        /// <exception cref="SchemaRegistryInternalException">
        ///     Error code 50001 – Error in the backend data store
        /// </exception>
        public Task<Config> GetGlobalCompatibilityAsync()
            => RequestToAsync<Config>("/config", HttpMethod.Get);

        /// <summary>
        /// </summary>
        /// <exception cref="IOException">
        ///     Aucune instance n'a pu exécuter la requête
        /// </exception>
        /// <exception cref="SchemaRegistryInternalException">
        ///     Error code 50001 – Error in the backend data store
        ///     Error code 40401 – Subject not found
        /// </exception>
        public Task<Config> GetCompatibilityAsync(string subject)
            => RequestToAsync<Config>($"/config/{subject}", HttpMethod.Get);

        /// <summary>
        /// </summary>
        /// <param name="compatibility">
        ///     FULL, BACKWARD or FORWARD
        /// </param>
        /// <returns>
        ///     New global compatibility level
        /// </returns>
        /// <exception cref="IOException">
        ///     Aucune instance n'a pu exécuter la requête
        /// </exception>
        /// <exception cref="SchemaRegistryInternalException">
        ///     L'instance de schema Registry a renvoyée une erreur interne
        /// </exception>
        public Task<Config> PutGlobalCompatibilityAsync(Config.Compatbility compatibility)
            => RequestToAsync<Config>("/config", HttpMethod.Put, new Config(compatibility));

        /// <summary>
        ///     Update compatibility level for the specified subject.
        /// </summary>
        /// <param name="subject">
        ///     Name of the subject
        /// </param>
        /// <param name="compatibility">
        ///     New global compatibility level. Must be one of NONE, FULL, FORWARD, BACKWARD
        /// </param>
        /// <returns>
        ///     New compatibility level of the subject
        /// </returns>
        /// <exception cref="IOException">
        ///     Aucune instance n'a pu exécuter la requête
        /// </exception>
        /// <exception cref="SchemaRegistryInternalException">
        ///     L'instance de schema Registry a renvoyée une erreur interne
        /// </exception>
        public Task<Config> PutCompatibilityAsync(string subject, Config.Compatbility compatibility)
            => RequestToAsync<Config>($"/config/{subject}", HttpMethod.Put, new Config(compatibility));
            
        #endregion Config
    }
}
