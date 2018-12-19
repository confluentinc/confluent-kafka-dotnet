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


namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     AvroSerdeProvider configuration properties.
    /// </summary>
    public class AvroSerdeProviderConfig : SchemaRegistryConfig
    {
        /// <summary>
        ///     Configuration property names specific the avro serde provider.
        /// </summary>
        public new static class PropertyNames
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
            public const string AvroSerializerBufferBytes = "avro.serializer.buffer.bytes";

            /// <summary>
            ///     Specifies whether or not the Avro serializer should attempt to auto-register
            ///     unrecognized schemas with Confluent Schema Registry.
            ///
            ///     default: true
            /// </summary>
            public const string AvroSerializerAutoRegisterSchemas = "avro.serializer.auto.register.schemas";
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
        public int? AvroSerializerBufferBytes
        {
            get { return GetInt(PropertyNames.AvroSerializerBufferBytes); }
            set { SetObject(PropertyNames.AvroSerializerBufferBytes, value); }
        }

        /// <summary>
        ///     Specifies whether or not the Avro serializer should attempt to auto-register
        ///     unrecognized schemas with Confluent Schema Registry.
        ///
        ///     default: true
        /// </summary>
        public bool? AvroSerializerAutoRegisterSchemas
        {
            get { return GetBool(PropertyNames.AvroSerializerAutoRegisterSchemas); }
            set { SetObject(PropertyNames.AvroSerializerAutoRegisterSchemas, value); }
        }
    }
}