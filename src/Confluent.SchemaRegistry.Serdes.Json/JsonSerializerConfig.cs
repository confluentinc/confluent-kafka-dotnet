// Copyright 2020 Confluent Inc.
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
using System.Linq;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     <see cref="Confluent.SchemaRegistry.Serdes.JsonSerializer{T}" />
    ///     configuration properties.
    /// </summary>
    public class JsonSerializerConfig : SerdeConfig
    {
        /// <summary>
        ///     Configuration property names specific to 
        ///     <see cref="Confluent.SchemaRegistry.Serdes.JsonSerializer{T}" />.
        /// </summary>
        public static class PropertyNames
        {
            /// <summary>
            ///     Specifies the initial size (in bytes) of the buffer used for JSON message
            ///     serialization. Use a value high enough to avoid resizing the buffer, but
            ///     small enough to avoid excessive memory use. Inspect the size of the byte
            ///     array returned by the Serialize method to estimate an appropriate value.
            ///     Note: each call to serialize creates a new buffer.
            ///
            ///     default: 1024
            /// </summary>
            public const string BufferBytes = "json.serializer.buffer.bytes";

            /// <summary>
            ///     Specifies whether or not the JSON serializer should attempt to auto-register
            ///     unrecognized schemas with Confluent Schema Registry.
            ///
            ///     default: true
            /// </summary>
            public const string AutoRegisterSchemas = "json.serializer.auto.register.schemas";

            /// <summary>
            ///     Specifies whether to normalize schemas, which will transform schemas
            ///     to have a consistent format, including ordering properties and references.
            ///
            ///     default: false
            /// </summary>
            public const string NormalizeSchemas = "json.serializer.normalize.schemas";

            /// <summary>
            ///     Specifies the schema ID to use for serialization.
            ///     WARNING: There is no check that the given schema is backwards compatible
            ///     with the schema of the object being serialized.
            /// </summary>
            public const string UseSchemaId = "json.serializer.use.schema.id";

            /// <summary>
            ///     Specifies whether or not the JSON serializer should use the latest subject
            ///     version for serialization.
            ///     WARNING: There is no check that the latest schema is compatible
            ///     with the schema of the object being serialized by default.
            ///     Use the LatestCompatibilityStrict config property to enable this.
            ///
            ///     default: false
            /// </summary>
            public const string UseLatestVersion = "json.serializer.use.latest.version";

            /// <summary>
            ///     Specifies whether or not the JSON serializer should check the compatibility 
            ///     with the latest schema of the subject if use.latest.version is set to true.
            ///
            ///     default: false
            /// </summary>
            public const string LatestCompatibilityStrict = "json.serializer.latest.compatibility.strict";
            
            ///     Specifies whether or not the JSON serializer should use the latest subject
            ///     version with the given metadata for serialization.
            ///     WARNING: There is no check that the latest schema is backwards compatible
            ///     with the schema of the object being serialized.
            /// </summary>
            public const string UseLatestWithMetadata = "json.serializer.use.latest.with.metadata";

            /// <summary>
            ///     The subject name strategy to use for schema registration / lookup.
            ///     Possible values: <see cref="Confluent.SchemaRegistry.SubjectNameStrategy" />
            /// </summary>
            public const string SubjectNameStrategy = "json.serializer.subject.name.strategy";

            /// <summary>
            ///     The schema id name strategy to use to serialize the ID/GUID.
            ///     Possible values: <see cref="Confluent.SchemaRegistry.SchemaIdSerializerStrategy" />
            /// </summary>
            public const string SchemaIdStrategy = "json.serializer.schema.id.strategy";

            /// <summary>
            ///     Specifies whether to validate payloads against the schema.
            ///
            ///     default: true
            /// </summary>
            public const string Validate = "json.serializer.validate";
        }


        /// <summary>
        ///     Initialize a new <see cref="JsonSerializerConfig" />.
        /// </summary>
        public JsonSerializerConfig() { }


        /// <summary>
        ///     Initialize a new <see cref="JsonSerializerConfig" /> from the provided
        ///     key/value pair collection.
        /// </summary>
        public JsonSerializerConfig(IEnumerable<KeyValuePair<string, string>> config) : base(config.ToDictionary(v => v.Key, v => v.Value)) { }


        /// <summary>
        ///     Specifies the initial size (in bytes) of the buffer used for JSON message
        ///     serialization. Use a value high enough to avoid resizing the buffer, but
        ///     small enough to avoid excessive memory use. Inspect the size of the byte
        ///     array returned by the Serialize method to estimate an appropriate value.
        ///     Note: each call to serialize creates a new buffer.
        /// 
        ///     default: 1024
        /// </summary>
        public int? BufferBytes
        {
            get { return GetInt(PropertyNames.BufferBytes); }
            set { SetObject(PropertyNames.BufferBytes, value); }
        }


        /// <summary>
        ///     Specifies whether or not the JSON serializer should attempt to auto-register
        ///     unrecognized schemas with Confluent Schema Registry.
        ///
        ///     default: true
        /// </summary>
        public bool? AutoRegisterSchemas
        {
            get { return GetBool(PropertyNames.AutoRegisterSchemas); }
            set { SetObject(PropertyNames.AutoRegisterSchemas, value); }
        }
        

        /// <summary>
        ///     Specifies whether to normalize schemas, which will transform schemas
        ///     to have a consistent format, including ordering properties and references.
        ///
        ///     default: false
        /// </summary>
        public bool? NormalizeSchemas
        {
            get { return GetBool(PropertyNames.NormalizeSchemas); }
            set { SetObject(PropertyNames.NormalizeSchemas, value); }
        }


        /// <summary>
        ///     Specifies the schema ID to be used for serialization.
        ///     WARNING: There is no check that the latest schema is backwards compatible
        ///     with the schema of the object being serialized.
        /// </summary>
        public int? UseSchemaId
        {
            get { return GetInt(PropertyNames.UseSchemaId); }
            set { SetObject(PropertyNames.UseSchemaId, value); }
        }


        /// <summary>
        ///     Specifies whether or not the JSON serializer should use the latest subject
        ///     version for serialization.
        ///     WARNING: There is no check that the latest schema is compatible
        ///     with the schema of the object being serialized by default.
        ///     Use the LatestCompatibilityStrict config property to enable this.
        ///
        ///     default: false
        /// </summary>
        public bool? UseLatestVersion
        {
            get { return GetBool(PropertyNames.UseLatestVersion); }
            set { SetObject(PropertyNames.UseLatestVersion, value); }
        }


        /// <summary>
        ///     Specifies whether or not the JSON serializer should check the backwards compatibility 
        ///     with the latest schema of the subject.
        ///
        ///     default: false
        /// </summary>
        public bool? LatestCompatibilityStrict
        {
            get { return GetBool(PropertyNames.LatestCompatibilityStrict); }
            set { SetObject(PropertyNames.LatestCompatibilityStrict, value); }
        }
        

        /// <summary>
        ///     Specifies whether or not the JSON serializer should use the latest schema
        ///     with the given metadata for serialization.
        ///     WARNING: There is no check that the latest schema is backwards compatible
        ///     with the schema of the object being serialized.
        /// </summary>
        public IDictionary<string, string> UseLatestWithMetadata
        {
            get { return GetDictionaryProperty(PropertyNames.UseLatestWithMetadata); }
            set { SetDictionaryProperty(PropertyNames.UseLatestWithMetadata, value); }
        }
        

        /// <summary>
        ///     Subject name strategy.
        ///     
        ///     default: SubjectNameStrategy.Topic
        /// </summary>
        public SubjectNameStrategy? SubjectNameStrategy
        {
            get
            {
                var r = Get(PropertyNames.SubjectNameStrategy);
                if (r == null) { return null; }
                else
                {
                    SubjectNameStrategy result;
                    if (!Enum.TryParse<SubjectNameStrategy>(r, out result))
                        throw new ArgumentException(
                            $"Unknown ${PropertyNames.SubjectNameStrategy} value: {r}.");
                    else
                        return result;
                }
            }
            set
            {
                if (value == null) { this.properties.Remove(PropertyNames.SubjectNameStrategy); }
                else { this.properties[PropertyNames.SubjectNameStrategy] = value.ToString(); }
            }
        }


        /// <summary>
        ///     Schema id strategy.
        ///
        ///     default: SchemaIdSerializerStrategy.Prefix
        /// </summary>
        public SchemaIdSerializerStrategy? SchemaIdStrategy
        {
            get
            {
                var r = Get(PropertyNames.SchemaIdStrategy);
                if (r == null) { return null; }
                else
                {
                    SchemaIdSerializerStrategy result;
                    if (!Enum.TryParse<SchemaIdSerializerStrategy>(r, out result))
                        throw new ArgumentException(
                            $"Unknown ${PropertyNames.SchemaIdStrategy} value: {r}.");
                    else
                        return result;
                }
            }
            set
            {
                if (value == null) { this.properties.Remove(PropertyNames.SchemaIdStrategy); }
                else { this.properties[PropertyNames.SchemaIdStrategy] = value.ToString(); }
            }
        }


        /// <summary>
        ///     Specifies whether or not the JSON serializer should attempt to
        ///     validate the payload against the schema.
        ///
        ///     default: true
        /// </summary>
        public bool? Validate
        {
            get { return GetBool(PropertyNames.Validate); }
            set { SetObject(PropertyNames.Validate, value); }
        }
    }
}
