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

using System.Collections.Generic;
using System.IO;
using System.Net;
using Avro.Generic;
using Avro.IO;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka.SchemaRegistry.Serialization
{
    /// <summary>
    ///     Avro Deserializer with Schema Registry integration
    /// </summary>
    /// <remarks>
    ///     Messages are formated in line with the confluentinc java implementation for compatibility:
    ///     - [0]: magic byte
    ///     - [1-4]: (big endian encoded) unique global id of the avro schema (as registered in schema registry)
    /// </remarks>
    public class AvroDeserializer : IDeserializer<object>
    {

        // topic refer to kafka topic
        // subject refers to schema registry subject. Usually topic postfixed by -key or -data

        public const byte MAGIC_BYTE = 0;
        
        private ISchemaRegistryClient SchemaRegisterClient { get; }
        
        // maintain a cache of deserializer, so that we only have to construct it once
        private readonly Dictionary<int, GenericReader<object>> readerBySchemaId = new Dictionary<int, GenericReader<object>>();
        
        /// <summary>
        ///     Initiliaze an avro serializer.
        /// </summary>
        /// <param name="schemaRegisterClient"></param>
        public AvroDeserializer(ISchemaRegistryClient schemaRegisterClient)
        {
            SchemaRegisterClient = schemaRegisterClient;
        }
        
        /// <summary>
        ///     Deserialize array to given tip
        /// </summary>
        /// <param name="array"></param>
        /// <param name="topic">
        ///     kafka topic, not used for avro deserialization
        /// </param>
        /// <returns>
        ///     a GenericRecord, GenericEnum, GenericFixed or a primitive type given the schema
        /// </returns>
        /// <exception cref="System.Runtime.Serialization.SerializationException">
        ///     Schemas do not match
        /// </exception>
        public object Deserialize(string topic, byte[] array)
        {
            // topic not necessary for deserialization (knowing if it's key or not neither)
            // we only care about schema id
            
            using (var stream = new MemoryStream(array))
            using (var reader = new BinaryReader(stream))
            {
                int magicByte = reader.ReadByte();
                if (magicByte != MAGIC_BYTE)
                {
                    //may change in the future with new format
                    throw new InvalidDataException("magic byte should be 0");
                }
                int writerId = IPAddress.NetworkToHostOrder(reader.ReadInt32());
                if (!readerBySchemaId.TryGetValue(writerId, out GenericReader<object> seralizer))
                {
                    // GetSchemaAsync may throw
                    string writerSchema = SchemaRegisterClient.GetSchemaAsync(writerId).Result;
                    var schema = Avro.Schema.Parse(writerSchema);
                        
                    // can be of multiple type: Record, Primitive...
                    // we don't read against a given schema, so writer and reader schema are same
                    seralizer = new GenericReader<object>(schema, schema);
                    readerBySchemaId[writerId] = seralizer;
                }
                return seralizer.Read(seralizer, new BinaryDecoder(stream));
            }
        }
       
        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
        {
            return config;
        }
    }
}
