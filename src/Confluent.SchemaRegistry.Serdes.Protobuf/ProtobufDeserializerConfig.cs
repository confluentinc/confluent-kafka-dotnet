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

using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     <see cref="Confluent.SchemaRegistry.Serdes.ProtobufDeserializer{T}" />
    ///     configuration properties.
    /// </summary>
    public class ProtobufDeserializerConfig : Config
    {
        /// <summary>
        ///     Configuration property names specific to 
        ///     <see cref="Confluent.SchemaRegistry.Serdes.ProtobufDeserializer{T}" />.
        /// </summary>
        public static class PropertyNames
        {
            /// <summary>
            ///     Specifies whether the Protobuf deserializer should deserialize message indexes
            ///     without zig-zag encoding.
            ///
            ///     default: false
            /// </summary>
            public const string UseDeprecatedFormat = "protobuf.deserializer.use.deprecated.format";
        }
        
        /// <summary>
        ///     Initialize a new <see cref="ProtobufDeserializerConfig" />.
        /// </summary>
        public ProtobufDeserializerConfig() { }


        /// <summary>
        ///     Initialize a new <see cref="ProtobufDeserializerConfig" /> from the provided
        ///     key/value pair collection.
        /// </summary>
        public ProtobufDeserializerConfig(IEnumerable<KeyValuePair<string, string>> config) : base(config.ToDictionary(v => v.Key, v => v.Value)) { }


        /// <summary>
        ///     Specifies whether the Protobuf deserializer should deserialize message indexes
        ///     without zig-zag encoding.
        ///
        ///     default: false
        /// </summary>
        public bool? UseDeprecatedFormat
        {
            get { return GetBool(PropertyNames.UseDeprecatedFormat); }
            set { SetObject(PropertyNames.UseDeprecatedFormat, value); }
        }
    }
}
