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
using System.Linq;
using Confluent.Kafka;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     <see cref="Confluent.SchemaRegistry.Serdes.AvroSerializer{T}" />
    ///     configuration properties.
    /// </summary>
    public class AvroSerializerConfig : Config
    {
        /// <summary>
        ///     Configuration property names specific to 
        ///     <see cref="Confluent.SchemaRegistry.Serdes.AvroSerializer{T}" />.
        /// </summary>
        public static class PropertyNames
        {
            /// <summary>
            ///     Specifies the initial size (in bytes) of the buffer used for Avro message
            ///     serialization. Use a value high enough to avoid resizing the buffer, but
            ///     small enough to avoid excessive memory use. Inspect the size of the byte
            ///     array returned by the Serialize method to estimate an appropriate value.
            ///     Note: each call to serialize creates a new buffer.
            ///
            ///     default: 1024
            /// </summary>
            public const string BufferBytes = "avro.serializer.buffer.bytes";

            /// <summary>
            ///     Specifies whether or not the Avro serializer should attempt to auto-register
            ///     unrecognized schemas with Confluent Schema Registry.
            ///
            ///     default: true
            /// </summary>
            public const string AutoRegisterSchemas = "avro.serializer.auto.register.schemas";

            /// <summary>
            ///     Specifies whether to normalize schemas, which will transform schemas
            ///     to have a consistent format, including ordering properties and references.
            ///
            ///     default: false
            /// </summary>
            public const string NormalizeSchemas = "avro.serializer.normalize.schemas";

            /// <summary>
            ///     Specifies whether or not the Avro serializer should use the latest subject
            ///     version for serialization.
            ///     WARNING: There is no check that the latest schema is backwards compatible
            ///     with the schema of the object being serialized.
            ///
            ///     default: false
            /// </summary>
            public const string UseLatestVersion = "avro.serializer.use.latest.version";

            /// <summary>
            ///     The subject name strategy to use for schema registration / lookup.
            ///     Possible values: <see cref="Confluent.SchemaRegistry.SubjectNameStrategy" />
            /// </summary>
            public const string SubjectNameStrategy = "avro.serializer.subject.name.strategy";
        }


        /// <summary>
        ///     Initialize a new <see cref="AvroSerializerConfig" />.
        /// </summary>
        public AvroSerializerConfig() { }


        /// <summary>
        ///     Initialize a new <see cref="AvroSerializerConfig" /> from the provided
        ///     key/value pair collection.
        /// </summary>
        public AvroSerializerConfig(IEnumerable<KeyValuePair<string, string>> config) : base(config.ToDictionary(v => v.Key, v => v.Value)) { }


        /// <summary>
        ///     Specifies the initial size (in bytes) of the buffer used for Avro message
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
        ///     Specifies whether or not the Avro serializer should attempt to auto-register
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
        ///     Specifies whether or not the Avro serializer should use the latest subject
        ///     version for serialization.
        ///     WARNING: There is no check that the latest schema is backwards compatible
        ///     with the schema of the object being serialized.
        ///
        ///     default: false
        /// </summary>
        public bool? UseLatestVersion
        {
            get { return GetBool(PropertyNames.UseLatestVersion); }
            set { SetObject(PropertyNames.UseLatestVersion, value); }
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

    }
}
