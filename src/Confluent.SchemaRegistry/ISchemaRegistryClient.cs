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
        /// <summary>
        ///     The maximum capacity of the local schema cache.
        /// </summary>
        int MaxCachedSchemas { get; }


        /// <summary>
        ///     Register a schema or get the schema id if it's already 
        ///     registered.
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
        Task<int> RegisterSchemaAsync(string subject, string schema);


        /// <summary>
        ///   Get the unique id of the specified schema registered against 
        ///   the specified subject.
        /// </summary>
        /// <param name="subject">
        ///   The subject the schema is registered against.
        /// </param>
        /// <param name="schema">
        ///   The schema to get the id for.
        /// </param>
        /// <returns>
        ///   The unique id identifying the schema.
        /// </returns>
        /// <exception cref="SchemaRegistryException">
        ///   Thrown if the schema is not registered against the subject.
        /// </exception>
        Task<int> GetSchemaIdAsync(string subject, string schema);


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
        ///     The latest schema registered against <paramref name="subject" />.
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
        ///     Gets a list of versions registered under the specified <paramref name="subject" />.
        /// </summary>
        /// <param name="subject">
        ///     The subject to get versions registered under.
        /// </param>
        /// <returns>
        ///     A list of versions registered under the specified <paramref name="subject" />.
        /// </returns>
        Task<List<int>> GetSubjectVersionsAsync(string subject);


        /// <summary>
        ///     Check if a schema is compatible with latest version registered against a 
        ///     specified subject.
        /// </summary>
        /// <param name="subject">
        ///     The subject to check.
        /// </param>
        /// <param name="schema">
        ///     The schema to check.
        /// </param>
        /// <returns>
        ///     true if <paramref name="schema" /> is compatible with the latest version 
        ///     registered against a specified subject, false otherwise.
        /// </returns>
        Task<bool> IsCompatibleAsync(string subject, string schema);


        /// <summary>
        ///     Returns the schema registry key subject name given a topic name.
        /// </summary>
        /// <param name="topic">
        ///     The topic name.
        /// </param>
        /// <param name="recordType">
        ///     The fully qualified Avro record type or null if the key is not
        ///     an Avro record type.
        /// </param>
        /// <returns>
        ///     The key subject name given a topic name.
        /// </returns>
        string ConstructKeySubjectName(string topic, string recordType = null);


        /// <summary>
        ///     Returns the schema registry value subject name given a topic name.
        /// </summary>
        /// <param name="topic">
        ///     The topic name.
        /// </param>
        /// <param name="recordType">
        ///     The fully qualified Avro record type or null if the value is not
        ///     an Avro record type.
        /// </param>
        /// <returns>
        ///     The value subject name given a topic name.
        /// </returns>
        string ConstructValueSubjectName(string topic, string recordType = null);
    }
}
