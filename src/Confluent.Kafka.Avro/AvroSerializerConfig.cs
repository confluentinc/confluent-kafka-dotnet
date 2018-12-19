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

using System.Collections.Generic;
using Confluent.SchemaRegistry;


namespace Confluent.Kafka.AvroSerdes
{
    /// <summary>
    ///     <see cref="Confluent.Kafka.AvroSerdes.AvroSerializer{T}" />
    ///     configuration properties.
    /// </summary>
    public class AvroSerializerConfig : Config
    {
        /// <summary>
        ///     Configuration property names specific to 
        ///     <see cref="Confluent.Kafka.AvroSerdes.AvroSerializer{T}" />.
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
        }

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
    }
}
