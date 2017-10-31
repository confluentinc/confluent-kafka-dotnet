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
using System.Threading.Tasks;


namespace Confluent.Kafka.SchemaRegistry
{
    public interface ISchemaRegistryClient
    {
        /// <summary>
        ///     Register a schema.
        /// </summary>
        /// <param name="subject">
        ///     The subject to register the schema against.
        /// </param>
        /// <param name="schema">
        ///     The schema to register.
        /// </param>
        /// <returns>
        ///     A unique id identifying the schema.
        /// </returns>
        Task<int> RegisterAsync(string subject, string schema);

        /// <summary>
        ///     Gets the schema uniquely identified by <paramref name="id" />.
        /// </summary>
        /// <param name="id">
        ///     The unique id of schema to get.
        /// </param>
        /// <returns>
        ///     The schema identified by <paramref name="id" />.
        /// </returns>
        Task<string> GetSchemaAsync(int id);

        /// <summary>
        ///     Gets a schema given a <paramref name="subject" /> and <paramref name="version" /> number.
        /// </summary>
        /// <param name="subject">
        ///     The subject to get the schema for.
        /// </param>
        /// <param name="version">
        ///     The version number of schema to get.
        /// </param>
        /// <returns>
        ///     The schema identified by the specified <paramref name="subject" /> and <paramref name="version" />.
        /// </returns>
        Task<string> GetSchemaAsync(string subject, int version);

        /// <summary>
        ///     Get the latest schema registered against the specified <paramref name="subject" />.
        /// </summary>
        /// <param name="subject">
        ///     The subject to get the latest associated schema for.
        /// </param>
        /// <returns>
        ///     The latest schema registred against <paramref name="subject" />.
        /// </returns>
        Task<Schema> GetLatestSchemaAsync(string subject);

        /// <summary>
        ///     Gets a list of all subjects with registered schemas.
        /// </summary>
        /// <returns>
        ///     A list of all subjects with registered schemas.
        /// </returns>
        Task<List<string>> GetAllSubjectsAsync();
        
        /// <summary>
        ///     Check if a schema is compatible with latest version registered against a 
        ///     specified subject.
        /// </summary>
        /// <param name="subject">
        ///     The subject to check.
        /// </param>
        /// <param name="avroSchema">
        ///     The schema to check.
        /// </param>
        /// <returns>
        ///     true if <paramref name="avroSchema" /> is compatible with the latest version 
        ///     registered against a specified subject, false otherwise.
        /// </returns>
        Task<bool> IsCompatibleAsync(string subject, string avroSchema);

        /// <summary>
        ///     Returns the schema registry subject name given a topic name and a subject 
        ///     type (key or value).
        /// </summary>
        /// <param name="topic">
        ///     The topic name.
        /// </param>
        /// <param name="keyOrValue">
        ///     The subject type (key or value).
        /// </param>
        /// <returns>
        ///     The subject name given a topic name and a subject 
        ///     type (key or value).
        /// </returns>
        string GetRegistrySubject(string topic, SubjectType keyOrValue);


        // TODO: the following interfaces may be required.
        
        // Task<bool> CheckSchemaAsync(string subject, string schema);
        // Task<Config.Compatbility> GetGlobalCompatibility();
        // Task<Config.Compatbility> GetCompatibilityAsync(string subject);
        // Task<Config.Compatbility> PutGlobalCompatibilityAsync(Config.Compatbility compatibility);
        // Task<Config.Compatbility> UpdateCompatibilityAsync(string subject, Config.Compatbility compatibility);
    }
}
