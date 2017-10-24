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
        ///     Register schema in registry
        /// </summary>
        /// <param name="subject"></param>
        /// <param name="schema"></param>
        /// <returns>
        ///     schema id (not version)
        /// </returns>
        Task<int> RegisterAsync(string subject, string schema);

        /// <summary>
        ///     Get schema by unique id
        /// </summary>
        /// <param name="id">
        ///     Unique id of s
        /// </param>
        /// <returns></returns>
        Task<string> GetSchemaAsync(int id);

        /// <summary>
        ///     Get schema by subject and version
        /// </summary>
        /// <param name="subject"></param>
        /// <param name="version">
        ///     version Id under this subject (positive)
        /// </param>
        /// <returns></returns>
        Task<string> GetSchemaAsync(string subject, int version);

        /// <summary>
        ///     Get latest known schema for the subject
        /// </summary>
        /// <param name="subject"></param>
        /// <returns></returns>
        Task<Schema> GetLatestSchemaAsync(string subject);

        /// <summary>
        ///     Get all subjects in registry
        /// </summary>
        /// <returns></returns>
        Task<List<string>> GetAllSubjectsAsync();
        
        /// <summary>
        ///     Check a schema is compatible with latest version in registry
        /// </summary>
        /// <param name="subject"></param>
        /// <param name="avroSchema"></param>
        /// <returns></returns>
        Task<bool> IsCompatibleAsync(string subject, string avroSchema);

        string GetRegistrySubject(string topic, bool isKey);

        //TODO: see if we need to add interfaces
        /*
        Task<bool> CheckSchemaAsync(string subject, string schema);
        Task<Config.Compatbility> GetGlobalCompatibility();
        Task<Config.Compatbility> GetCompatibilityAsync(string subject);
        Task<Config.Compatbility> PutGlobalCompatibilityAsync(Config.Compatbility compatibility);
        Task<Config.Compatbility> UpdateCompatibilityAsync(string subject, Config.Compatbility compatibility);
        */
    }
}