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
        ///     Register an Avro schema or get the schema id if it's already 
        ///     registered.
        /// </summary>
        /// <param name="subject">
        ///     The subject to register the schema against.
        /// </param>
        /// <param name="avroSchema">
        ///     The schema to register.
        /// </param>
        /// <param name="normalize">
        ///     Whether to normalize schemas.
        /// </param>
        /// <returns>
        ///     A unique id identifying the schema.
        /// </returns>
        [Obsolete("Superseded by RegisterSchemaAsync(string, Schema, bool)")]
        Task<int> RegisterSchemaAsync(string subject, string avroSchema, bool normalize = false);


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
        /// <param name="normalize">
        ///     Whether to normalize schemas.
        /// </param>
        /// <returns>
        ///     A unique id identifying the schema.
        /// </returns>
        Task<int> RegisterSchemaAsync(string subject, Schema schema, bool normalize = false);

        /// <summary>
        ///   Get the unique id of the specified avro schema registered against 
        ///   the specified subject.
        /// </summary>
        /// <param name="subject">
        ///   The subject the schema is registered against.
        /// </param>
        /// <param name="avroSchema">
        ///   The schema to get the id for.
        /// </param>
        /// <param name="normalize">
        ///   Whether to normalize schemas.
        /// </param>
        /// <returns>
        ///   The unique id identifying the schema.
        /// </returns>
        /// <exception cref="SchemaRegistryException">
        ///   Thrown if the schema is not registered against the subject.
        /// </exception>
        [Obsolete("Superseded by GetSchemaIdAsync(string, Schema, bool)")]
        Task<int> GetSchemaIdAsync(string subject, string avroSchema, bool normalize = false);


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
        /// <param name="normalize">
        ///   Whether to normalize schemas.
        /// </param>
        /// <returns>
        ///   The unique id identifying the schema.
        /// </returns>
        /// <exception cref="SchemaRegistryException">
        ///   Thrown if the schema is not registered against the subject.
        /// </exception>
        Task<int> GetSchemaIdAsync(string subject, Schema schema, bool normalize = false);


        /// <summary>
        ///     Gets the schema uniquely identified by <paramref name="id" />.
        /// </summary>
        /// <param name="id">
        ///     The unique id of schema to get.
        /// </param>
        /// <param name="format">
        ///     The format of the schema to get. Currently, the only supported
        ///     value is "serialized", and this is only valid for protobuf
        ///     schemas. If 'serialized', the SchemaString property of the returned
        ///     value will be a base64 encoded protobuf file descriptor. If null,
        ///     SchemaString will be human readable text.
        /// </param>
        /// <returns>
        ///     The schema identified by <paramref name="id" />.
        /// </returns>
        Task<Schema> GetSchemaAsync(int id, string format = null);


        /// <summary>
        ///     Get the registered schema details (including version and id)
        ///     given a subject name and schema, or throw an exception if
        ///     the schema is not registered against the subject.
        /// </summary>
        /// <param name="subject">
        ///     The subject name the schema is registered against.
        /// </param>
        /// <param name="schema">
        ///     The schema to lookup.
        /// </param>
        /// <param name="ignoreDeletedSchemas">
        ///     Whether or not to ignore deleted schemas.
        /// </param>
        /// <param name="normalize">
        ///     Whether to normalize schemas.
        /// </param>
        /// <returns>
        ///     The schema identified by the specified <paramref name="subject" /> and <paramref name="schema" />.
        /// </returns>
        Task<RegisteredSchema> LookupSchemaAsync(string subject, Schema schema, bool ignoreDeletedSchemas, bool normalize = false);


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
        Task<RegisteredSchema> GetRegisteredSchemaAsync(string subject, int version);


        /// <summary>
        ///     DEPRECATED. Superseded by GetRegisteredSchemaAsync(string subject, int version)
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
        [Obsolete("Superseded by GetRegisteredSchemaAsync(string subject, int version). This method will be removed in a future release.")]
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
        Task<RegisteredSchema> GetLatestSchemaAsync(string subject);


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
        ///     Check if an avro schema is compatible with latest version registered against a 
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
        [Obsolete("Superseded by IsCompatibleAsync(string, Schema)")]
        Task<bool> IsCompatibleAsync(string subject, string avroSchema);


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
        Task<bool> IsCompatibleAsync(string subject, Schema schema);


        /// <summary>
        ///     DEPRECATED. SubjectNameStrategy should now be specified via serializer configuration.
        ///     Returns the schema registry key subject name given a topic name.
        /// </summary>
        /// <param name="topic">
        ///     The topic name.
        /// </param>
        /// <param name="recordType">
        ///     The fully qualified record type. May be null if not required by
        ///     the configured subject naming strategy.
        /// </param>
        /// <returns>
        ///     The key subject name given a topic name.
        /// </returns>
        [Obsolete("SubjectNameStrategy should now be specified via serializer configuration. This method will be removed in a future release.")]
        string ConstructKeySubjectName(string topic, string recordType = null);


        /// <summary>
        ///     DEPRECATED. SubjectNameStrategy should now be specified via serializer configuration.
        ///     Returns the schema registry value subject name given a topic name.
        /// </summary>
        /// <param name="topic">
        ///     The topic name.
        /// </param>
        /// <param name="recordType">
        ///     The fully qualified record type. May be null if not required by
        ///     the configured subject naming strategy.
        /// </param>
        /// <returns>
        ///     The value subject name given a topic name.
        /// </returns>
        [Obsolete("SubjectNameStrategy should now be specified via serializer configuration. This method will be removed in a future release.")]
        string ConstructValueSubjectName(string topic, string recordType = null);


        /// <summary>
        ///     If the subject is specified returns compatibility type for the specified subject.
        ///     Otherwise returns global compatibility type.
        /// </summary>
        /// <param name="subject">
        ///     The subject to get the compatibility for.
        /// </param>
        /// <returns>
        ///     Compatibility type.
        /// </returns>
        Task<Compatibility> GetCompatibilityAsync(string subject = null);


        /// <summary>
        ///     If the subject is specified sets compatibility type for the specified subject.
        ///     Otherwise sets global compatibility type.
        /// </summary>
        /// <param name="subject">
        ///      The subject to set the compatibility for.
        /// </param>
        /// <param name="compatibility">
        ///     Compatibility type.
        /// </param>
        /// <returns>
        ///      New compatibility type.
        /// </returns>
        Task<Compatibility> UpdateCompatibilityAsync(Compatibility compatibility, string subject = null);
    }
}
