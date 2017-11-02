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

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka.SchemaRegistry.Rest.Entities;
using Confluent.Kafka.SchemaRegistry.Rest.Entities.Requests;


namespace Confluent.Kafka.SchemaRegistry.Rest
{
    /// <remarks>
    ///     It may be useful to expose this publically, but this is not
    ///     required by the Avro serializers, so keeping internal 
    ///     for now to minimize documentation / risk of API change etc.
    /// </remarks>
    internal interface ISchemaRegistyRestService : IDisposable
    {
        Task<Schema> CheckSchemaAsync(string subject, string schema);
        Task<Config> GetCompatibilityAsync(string subject);
        Task<Config> GetGlobalCompatibilityAsync();
        Task<Schema> GetLatestSchemaAsync(string subject);
        Task<SchemaString> GetSchemaAsync(int id);
        Task<Schema> GetSchemaAsync(string subject, int version);
        Task<List<string>> GetSubjectsAsync();
        Task<List<string>> GetSubjectVersions(string subject);
        Task<SchemaId> PostSchemaAsync(string subject, string schema);
        Task<Config> PutCompatibilityAsync(string subject, Config.Compatbility compatibility);
        Task<Config> PutGlobalCompatibilityAsync(Config.Compatbility compatibility);
        Task<CompatibilityCheckResponse> TestCompatibilityAsync(string subject, int versionId, string avroSchema);
        Task<CompatibilityCheckResponse> TestLatestCompatibilityAsync(string subject, string avroSchema);
    }
}