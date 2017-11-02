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
using System.IO;
using System.Net;
using Avro.Specific;
using Avro.IO;
using Avro.Generic;
using Confluent.Kafka.SchemaRegistry;


namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     Serializer to serialize all kind of object using confluent Schema Registry
    ///     For performances, use the generic serializer if you always use a specific type
    /// </summary>
    internal abstract class AvroSerializerBase<T> : ISerializer<T>
    {
        // [0] : Magic byte (0 as of today, used for future version with breaking change)
        // [1-4] : unique global id of avro schema used for write (as registered in schema registry), BIG ENDIAN
        // following: data serialized with corresponding schema

        // topic refer to kafka topic
        // subject refers to schema registry subject. Usually topi postfixed by -key or -data

        /// <summary>
        ///     Magic byte to identify avro/confluent protocol
        /// </summary>
        public const byte MAGIC_BYTE = 0;

        public bool IsKey { get; }
        public ISchemaRegistryClient SchemaRegistryClient { get; }

        // Deserializers against different versions of the schema (older or newer)
        private readonly Dictionary<int, DatumReader<object>> _avroDeserializerBySchemaId = new Dictionary<int, DatumReader<object>>();


        /// <summary>
        ///     Initiliaze an avro serializer.
        /// </summary>
        /// <param name="schemaRegistryClient"></param>
        /// <param name="isKey"></param>
        public AvroSerializerBase(ISchemaRegistryClient schemaRegistryClient, bool isKey)
        {
            SchemaRegistryClient = schemaRegistryClient;
            IsKey = isKey;
        }
        
        protected abstract Avro.Schema GetSchema(T data);
        protected abstract DefaultWriter GetWriter(T data);

        /// <summary>
        /// </summary>
        /// <param name="data"></param>
        /// <param name="topic"></param>
        /// <param name="length">Length of the result to take into account </param>
        /// <param name="isKey"></param>
        /// <returns></returns>
        public byte[] Serialize(string topic, T data)
        {
            string subject = SchemaRegistryClient.ConstructSubjectName(topic, IsKey);

            int schemaId;
            Avro.Schema schema = GetSchema(data);
            schemaId = SchemaRegistryClient.RegisterAsync(subject, schema.ToString()).Result;

            // TODO check to use something else than 30 which is not optimal.
            // For primitive type, we can "easily" generate an exact value
            using (var stream = new MemoryStream(30))
            {
                // 1 byte: magic byte
                stream.WriteByte(MAGIC_BYTE);

                // 4 bytes: schema global unique id
                // use network order to b compatible with other implementation
                byte[] idBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(schemaId));
                stream.Write(idBytes, 0, 4);

                DefaultWriter writer = GetWriter(data);
                writer.Write(data, new BinaryEncoder(stream));
                
                // TODO
                // stream.ToArray copy the memory stream to a new Array
                // we may rather want to use GetBuffer (or arraySegment in netstandard)
                // but we need array segment in serializer in this case
                return stream.ToArray();
            }
        }
        
        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
        {
            return config;
        }
    }
}
