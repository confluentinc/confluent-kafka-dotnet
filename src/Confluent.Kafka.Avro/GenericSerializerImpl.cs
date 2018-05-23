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

using System.Collections.Generic;
using System.IO;
using System.Net;
using Confluent.SchemaRegistry;
using Avro.Generic;
using Avro.IO;


namespace Confluent.Kafka.Serialization
{
    internal class GenericSerializerImpl : IAvroSerializerImpl<GenericRecord>
    {
        private ISchemaRegistryClient schemaRegistryClient;
        private bool autoRegisterSchema;
        private int initialBufferSize;
        private bool isKey;

        private Dictionary<string, KeyValuePair<string, int>> registeredSchemas = new Dictionary<string, KeyValuePair<string, int>>();

        private object serializeLockObj = new object();

        public GenericSerializerImpl(
            ISchemaRegistryClient schemaRegistryClient,
            bool autoRegisterSchema,
            int initialBufferSize,
            bool isKey)
        {
            this.schemaRegistryClient = schemaRegistryClient;
            this.autoRegisterSchema = autoRegisterSchema;
            this.initialBufferSize = initialBufferSize;
            this.isKey = isKey;
        }

        /// <summary>
        ///     Serialize GenericRecord instance to a byte array in avro format. The serialized
        ///     data is preceeded by a "magic byte" (1 byte) and the id of the schema as registered
        ///     in Confluent's Schema Registry (4 bytes, network byte order). This call may block or throw 
        ///     on first use for a particular topic during schema registration.
        /// </summary>
        /// <param name="topic">
        ///     The topic associated wih the data.
        /// </param>
        /// <param name="data">
        ///     The object to serialize.
        /// </param>
        /// <returns>
        ///     <paramref name="data" /> serialized as a byte array.
        /// </returns>
        public byte[] Serialize(string topic, GenericRecord data)
        {
            int schemaId;
            Avro.RecordSchema writerSchema;
            lock (serializeLockObj)
            {
                // TODO: If any of these caches fills up, this is probably an
                // indication of misuse of the serializer. Ideally we would do 
                // something more sophisticated than the below + not allow 
                // the misuse to keep happening without warning.
                if (registeredSchemas.Count > schemaRegistryClient.MaxCachedSchemas)
                {
                    registeredSchemas.Clear();
                }

                // Verify schema compatibility (& register as required) + get the 
                // id corresponding to the schema.
                // TODO: Again, the hash functions in use below are potentially 
                // slow since writerSchemaString is potentially long. It would be
                // better to use hash functions based on the writerSchemaString 
                // object reference, not value.

                writerSchema = data.Schema;

                string subject = this.isKey
                    ? schemaRegistryClient.ConstructKeySubjectName(topic)
                    : schemaRegistryClient.ConstructValueSubjectName(topic);
  
                if (!registeredSchemas.ContainsKey(subject))
                {
                    string writerSchemaString = writerSchema.ToString();
                    int schemaIdResult;
                    if (autoRegisterSchema)
                    {
                        schemaIdResult = schemaRegistryClient.RegisterSchemaAsync(subject, writerSchemaString).Result;
                    }
                    else
                    {
                        schemaIdResult = schemaRegistryClient.GetSchemaIdAsync(subject, writerSchemaString).Result;
                    }

                    registeredSchemas.Add(subject, new KeyValuePair<string, int>(writerSchemaString, schemaIdResult));
                }

                schemaId = registeredSchemas[subject].Value;
            }

            using (var stream = new MemoryStream(initialBufferSize))
            using (var writer = new BinaryWriter(stream))
            {
                stream.WriteByte(Constants.MagicByte);
                writer.Write(IPAddress.HostToNetworkOrder(schemaId));
                new GenericWriter<GenericRecord>(writerSchema)
                    .Write(data, new BinaryEncoder(stream));
                return stream.ToArray();
            }
        }
    }
}
