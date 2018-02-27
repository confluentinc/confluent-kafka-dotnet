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
    /// <summary>
    ///     An interface implemented by Confluent Schema Registry clients.
    /// </summary>
    public interface ISchemaRegistryClient : IDisposable
    {
        /// <include file='include_docs.xml' path='API.Member[@name="ISchemaRegistryClient_MaxCachedSchemas"]/*' />
        int MaxCachedSchemas { get; }

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_RegisterAsync"]/*' />
        Task<int> RegisterSchemaAsync(string subject, string schema);

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetIdAsync"]/*' />
        Task<int> GetSchemaIdAsync(string subject, string schema);

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetSchemaAsync"]/*' />
        Task<string> GetSchemaAsync(int id);

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetSchemaAsyncSubjectVersion"]/*' />
        Task<string> GetSchemaAsync(string subject, int version);

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetLatestSchemaAsync"]/*' />
        Task<Schema> GetLatestSchemaAsync(string subject);

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_GetAllSubjectsAsync"]/*' />
        Task<List<string>> GetAllSubjectsAsync();

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_IsCompatibleAsync"]/*' />
        Task<bool> IsCompatibleAsync(string subject, string schema);

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_ConstructKeySubjectName"]/*' />
        string ConstructKeySubjectName(string topic);

        /// <include file='include_docs.xml' path='API/Member[@name="ISchemaRegistryClient_ConstructValueSubjectName"]/*' />
        string ConstructValueSubjectName(string topic);
    }
}
