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
using System.Threading.Tasks;


namespace Confluent.SchemaRegistry
{
    /// <remarks>
    ///     It may be useful to expose this publicly, but this is not
    ///     required by the serializers, so we will keep this internal 
    ///     for now to minimize documentation / risk of API change etc.
    /// </remarks>
    internal interface IRestService : IDisposable
    {
        Task<Compatibility> GetCompatibilityAsync(string subject);
        Task<Compatibility> UpdateCompatibilityAsync(string subject, Compatibility compatibility);
        Task<RegisteredSchema> GetLatestSchemaAsync(string subject);
        Task<RegisteredSchema> GetLatestWithMetadataAsync(string subject, IDictionary<string, string> metadata, bool ignoreDeletedSchemas);
        Task<Schema> GetSchemaAsync(int id, string format = null);
        Task<Schema> GetSchemaBySubjectAndIdAsync(string subject, int id, string format = null);
        Task<Schema> GetSchemaByGuidAsync(string guid, string format = null);
        Task<RegisteredSchema> GetSchemaAsync(string subject, int version, bool ignoreDeletedSchemas = true);
        Task<List<string>> GetSubjectsAsync();
        Task<List<int>> GetSubjectVersionsAsync(string subject);
        Task<int> RegisterSchemaAsync(string subject, Schema schema, bool normalize);
        Task<RegisteredSchema> RegisterSchemaWithResponseAsync(string subject, Schema schema, bool normalize);
        Task<bool> TestCompatibilityAsync(string subject, int versionId, Schema schema);
        Task<bool> TestLatestCompatibilityAsync(string subject, Schema schema);
        Task<RegisteredSchema> LookupSchemaAsync(string subject, Schema schema, bool ignoreDeletedSchemas, bool normalize);
        Task<List<Association>> GetAssociationsByResourceNameAsync(
            string resourceName,
            string resourceNamespace,
            string resourceType,
            List<string> associationTypes,
            string lifecycle,
            int offset,
            int limit);
    }
}
