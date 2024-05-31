// Copyright 2018 Confluent Inc.
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
    ///     <see cref="Confluent.SchemaRegistry.Serdes.AvroDeserializer{T}" />
    ///     configuration properties.
    /// </summary>
    public class AvroDeserializerConfig : SerdeConfig
    {
        /// <summary>
        ///     Configuration property names specific to 
        ///     <see cref="Confluent.SchemaRegistry.Serdes.AvroDeserializer{T}" />.
        /// </summary>
        public static class PropertyNames
        {
            /// <summary>
            ///     Specifies whether or not the Avro deserializer should use the latest subject
            ///     version for deserialization.
            ///
            ///     default: false
            /// </summary>
            public const string UseLatestVersion = "avro.deserializer.use.latest.version";

            /// <summary>
            ///     Specifies whether or not the Avro deserializer should use the latest subject
            ///     version with the given metadata for deserialization.
            /// </summary>
            public const string UseLatestWithMetadata = "avro.deserializer.use.latest.with.metadata";

            /// <summary>
            ///     The subject name strategy to use for schema registration / lookup.
            ///     Possible values: <see cref="Confluent.SchemaRegistry.SubjectNameStrategy" />
            /// </summary>
            public const string SubjectNameStrategy = "avro.deserializer.subject.name.strategy";
        }


        /// <summary>
        ///     Initialize a new <see cref="AvroDeserializerConfig" />.
        /// </summary>
        public AvroDeserializerConfig() { }


        /// <summary>
        ///     Initialize a new <see cref="AvroDeserializerConfig" /> from the provided
        ///     key/value pair collection.
        /// </summary>
        public AvroDeserializerConfig(IEnumerable<KeyValuePair<string, string>> config) : base(config.ToDictionary(v => v.Key, v => v.Value)) { }

        
        /// <summary>
        ///     Specifies whether or not the Avro deserializer should use the latest subject
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
        ///     Specifies whether or not the Avro deserializer should use the latest subject
        ///     version with the given metadata for serialization.
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
    }
}
