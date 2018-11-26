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

using System.Threading.Tasks;
using System.Collections.Generic;
using Avro.Generic;
using Confluent.SchemaRegistry;
using System;
using System.Linq;


namespace Confluent.Kafka.AvroSerdes
{
    /// <summary>
    ///     Avro deserializer. Use this deserializer with GenericRecord, types 
    ///     generated using the avrogen.exe tool or one of the following 
    ///     primitive types: int, long, float, double, boolean, string, byte[].
    /// </summary>
    /// <remarks>
    ///     Serialization format:
    ///       byte 0:           Magic byte use to identify the protocol format.
    ///       bytes 1-4:        Unique global id of the Avro schema that was used for encoding (as registered in Confluent Schema Registry), big endian.
    ///       following bytes:  The serialized data.
    /// </remarks>
    public class AvroDeserializer<T>
    {
        private IAvroDeserializerImpl<T> deserializerImpl;


        /// <summary>
        ///     Initiliaze a new AvroDeserializer instance.
        /// </summary>
        /// <param name="config">
        ///     Deserializer configuration properties (refer to 
        ///     <see cref="AvroDeserializerConfig" />).
        /// </param>
        public AvroDeserializer(IEnumerable<KeyValuePair<string, string>> config = null)
        {
            if (config == null) { return; }

            var nonAvroConfig = config.Where(item => !item.Key.StartsWith("avro."));
            if (nonAvroConfig.Count() > 0)
            {
                throw new ArgumentException($"AvroDeserializer: unknown configuration parameter {nonAvroConfig.First().Key}.");
            }

            var avroConfig = config.Where(item => item.Key.StartsWith("avro."));
            if (avroConfig.Count() != 0)
            {
                throw new ArgumentException($"AvroDeserializer: unknown configuration parameter {avroConfig.First().Key}");
            }
        }

        /// <summary>
        ///     Deserialize an object of type <typeparamref name="T"/> from a byte array.
        /// </summary>
        /// <param name="topic">
        ///     The topic associated with the data (ignored by this deserializer).
        /// </param>
        /// <param name="data">
        ///     A byte array containing the object serialized in the format produced
        ///     by <see cref="AvroSerializer{T}" />.
        /// </param>
        /// <param name="isKey">
        ///     True if deserializing message key data, false otherwise.
        /// </param>
        /// <param name="schemaRegistryClient">
        ///     An implementation of ISchemaRegistryClient used for
        ///     communication with Confluent Schema Registry.
        /// </param>
        /// <returns>
        ///     The deserialized <typeparamref name="T"/> value.
        /// </returns>
        public async Task<T> Deserialize(ISchemaRegistryClient schemaRegistryClient, string topic, byte[] data, bool isKey)
        {
            try
            {
                if (deserializerImpl == null)
                {
                    deserializerImpl = (typeof(T) == typeof(GenericRecord))
                        ? (IAvroDeserializerImpl<T>)new GenericDeserializerImpl(schemaRegistryClient)
                        : new SpecificDeserializerImpl<T>(schemaRegistryClient);
                }

                return await deserializerImpl.Deserialize(topic, data.ToArray());
            }
            catch (AggregateException e)
            {
                throw new DeserializationException("Error occured deserializing Avro data.", e.InnerException);
            }
            catch (Exception e)
            {
                throw new DeserializationException("Error occured deserializing Avro data.", e);
            }
        }

    }
}
