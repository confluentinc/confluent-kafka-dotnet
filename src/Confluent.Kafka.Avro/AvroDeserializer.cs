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
using Avro.Specific;
using Confluent.Kafka.SchemaRegistry;
using System.Reflection;
using System;

namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     Data with associated avro schema
    /// </summary>
    public class AvroRecord
    {
        public Avro.Schema Schema { get; }
        public object Data { get; }

        public AvroRecord(Avro.Schema schema, object data)
        {
            Schema = schema;
            Data = data;
        }
    }

    /// <summary>
    ///     Data with associated schema registry id
    /// </summary>
    public class DataWithId
    {
        public int WriterId { get; }
        public object Data { get; }

        public DataWithId(int writerId, object data)
        {
            WriterId = writerId;
            Data = data;
        }
    }

    /// <summary>
    ///     Avro specific deserializer
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class AvroDeserializer<T> : AvroDeserializer, IDeserializer<T>
    {
        Avro.Schema readerSchema;

        public AvroDeserializer(ISchemaRegistryClient schemaRegisterClient) : base(schemaRegisterClient)
        {
            Type readerType = typeof(T);
            if(readerType.IsSubclassOf(typeof(ISpecificRecord)))
            {
                readerSchema = (Avro.Schema)typeof(T).GetField("_SCHEMA", BindingFlags.Public | BindingFlags.Static).GetValue(null);
            }
            else if(readerType.Equals(typeof(int)))
            {
                readerSchema = Avro.Schema.Parse("int");
            }
            else if (readerType.Equals(typeof(bool)))
            {
                readerSchema = Avro.Schema.Parse("boolean");
            }
            else if (readerType.Equals(typeof(double)))
            {
                readerSchema = Avro.Schema.Parse("double");
            }
            else if (readerType.Equals(typeof(string)))
            {
                readerSchema = Avro.Schema.Parse("string");
            }
            else if (readerType.Equals(typeof(float)))
            {
                readerSchema = Avro.Schema.Parse("float");
            }
            else if (readerType.Equals(typeof(long)))
            {
                readerSchema = Avro.Schema.Parse("long");
            }
            else if (readerType.Equals(typeof(byte[])))
            {
                readerSchema = Avro.Schema.Parse("bytes");
            }
            else
            {
                throw new NotImplementedException("TODO");
            }
        }

        /// <summary>
        ///     Create a GenericReader against the reader schema from the generic type.
        ///     The schema has to be compatible or this function will throw.
        /// </summary>
        /// <param name="writerSchema"></param>
        /// <returns></returns>
        protected override GenericReader<object> GenerateDeserializer(Avro.Schema writerSchema)
        {
            return new GenericReader<object>(writerSchema, readerSchema);
        }

        /// <summary>
        ///     Deserialize data to the given type
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        public T Deserialize(string topic, byte[] data)
        {
            return (T) Deserialize(topic, data, out Avro.Schema writerSchema, out int writerId);
        }
    }

    /// <summary>
    ///     Avro generic deserializer
    /// </summary>
    public class AvroDeserializer : IDeserializer<AvroRecord>, IDeserializer<object>, IDeserializer<DataWithId>
    {
        //We use the same format as confluentinc java implementation for compatibility :

        // [0] : Magic byte (0 as of today, used for future version with breaking change)
        // [1-4] : unique global id of avro schema used for write (as registered in schema registry), BIG ENDIAN
        // following: data serialized with corresponding schema

        // topic refer to kafka topic
        // subject refers to schema registry subject. Usually topic postfixed by -key or -data

        public const byte MAGIC_BYTE = 0;
        
        private SchemaRegistry.ISchemaRegistryClient SchemaRegisterClient { get; }
        
        // maintain a cache of deserializer, so that we only have to construct it once
        private readonly Dictionary<int, GenericReader<object>> readerBySchemaId = new Dictionary<int, GenericReader<object>>();
        
        /// <summary>
        ///     Initiliaze an avro serializer.
        /// </summary>
        /// <param name="schemaRegisterClient"></param>
        public AvroDeserializer(SchemaRegistry.ISchemaRegistryClient schemaRegisterClient)
        {
            SchemaRegisterClient = schemaRegisterClient;
        }

        /// <summary>
        ///     Generate a GenericReader.
        /// </summary>
        /// <param name="writerSchema">The writer schema</param>
        /// <returns></returns>
        protected virtual GenericReader<object> GenerateDeserializer(Avro.Schema writerSchema)
        {
            return new GenericReader<object>(writerSchema, writerSchema);
        }

        protected object Deserialize(string topic, byte[] array, out Avro.Schema schema, out int writerId)
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
                writerId = IPAddress.NetworkToHostOrder(reader.ReadInt32());
                if (!readerBySchemaId.TryGetValue(writerId, out GenericReader<object> genericReader))
                {
                    // GetSchemaAsync may throw
                    string writerSchema = SchemaRegisterClient.GetSchemaAsync(writerId).Result;
                    schema = Avro.Schema.Parse(writerSchema);

                    // can be of multiple type: Record, Primitive...
                    // we don't read against a given schema, so writer and reader schema are same
                    genericReader = GenerateDeserializer(schema); 
                    readerBySchemaId[writerId] = genericReader;
                }
                else
                {
                    schema = genericReader.ReaderSchema;
                }

                return genericReader.Read(genericReader, new BinaryDecoder(stream));
            }
        }

        /// <summary>
        ///     Deserialize array to given tip
        /// </summary>
        /// <param name="array"></param>
        /// <param name="topic">kafka topic, not used for avro deserialization</param>
        /// <returns>a GenericRecord, GenericEnum, GenericFixed or a primitive type given the schema</returns>
        /// <exception cref="System.Runtime.Serialization.SerializationException">Schemas do not match</exception>
        AvroRecord IDeserializer<AvroRecord>.Deserialize(string topic, byte[] array)
        {
            var data = Deserialize(topic, array, out Avro.Schema schema, out int writerId);
            return new AvroRecord(schema, data);
        }

        object IDeserializer<object>.Deserialize(string topic, byte[] array)
        {
            return Deserialize(topic, array, out Avro.Schema schema, out int writerId);
        }

        DataWithId IDeserializer<DataWithId>.Deserialize(string topic, byte[] array)
        {
            var data = Deserialize(topic, array, out Avro.Schema schema, out int writerId);
            return new DataWithId(writerId, data);
        }

        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
        {
            return config;
        }
    }




    /// <summary>
    ///     Avro generic deserializer
    /// </summary>
    public class AvroDeserializer1<T> : IDeserializer<T>
    {
        //We use the same format as confluentinc java implementation for compatibility :

        // [0] : Magic byte (0 as of today, used for future version with breaking change)
        // [1-4] : unique global id of avro schema used for write (as registered in schema registry), BIG ENDIAN
        // following: data serialized with corresponding schema

        // topic refer to kafka topic
        // subject refers to schema registry subject. Usually topic postfixed by -key or -data

        public const byte MAGIC_BYTE = 0;

        private ISchemaRegistryClient SchemaRegisterClient { get; }

        // maintain a cache of deserializer, so that we only have to construct it once
        private readonly Dictionary<int, DatumReader<T>> readerBySchemaId = new Dictionary<int, DatumReader<T>>();

        public Avro.Schema ReaderSchema { get; }
        /// <summary>
        ///     Initiliaze an avro serializer.
        /// </summary>
        /// <param name="schemaRegisterClient"></param>
        public AvroDeserializer1(ISchemaRegistryClient schemaRegisterClient)
        {
            SchemaRegisterClient = schemaRegisterClient;

            if (typeof(T).IsSubclassOf(typeof(ISpecificRecord)))
            {
                ReaderSchema = (Avro.Schema)typeof(T).GetField("_SCHEMA", BindingFlags.Public | BindingFlags.Static).GetValue(null);
            }
            else if (typeof(T).Equals(typeof(int)))
            {
                ReaderSchema = Avro.Schema.Parse("int");
            }
            else if (typeof(T).Equals(typeof(bool)))
            {
                ReaderSchema = Avro.Schema.Parse("boolean");
            }
            else if (typeof(T).Equals(typeof(double)))
            {
                ReaderSchema = Avro.Schema.Parse("double");
            }
            else if (typeof(T).Equals(typeof(string)))
            {
                ReaderSchema = Avro.Schema.Parse("string");
            }
            else if (typeof(T).Equals(typeof(float)))
            {
                ReaderSchema = Avro.Schema.Parse("float");
            }
            else if (typeof(T).Equals(typeof(long)))
            {
                ReaderSchema = Avro.Schema.Parse("long");
            }
            else if (typeof(T).Equals(typeof(byte[])))
            {
                ReaderSchema = Avro.Schema.Parse("bytes");
            }
            else
            {
                throw new NotImplementedException("TODO");
            }
        }

        /// <summary>
        ///     Generate a GenericReader.
        /// </summary>
        /// <param name="writerSchema">The writer schema</param>
        /// <returns></returns>
        protected virtual GenericReader<T> GenerateSerializer(Avro.Schema writerSchema)
        {
            return new GenericReader<T>(writerSchema, writerSchema);
        }

        protected T Deserialize(string topic, byte[] array, out Avro.Schema writerSchema, out int writerId)
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
                writerId = IPAddress.NetworkToHostOrder(reader.ReadInt32());
                if (!readerBySchemaId.TryGetValue(writerId, out DatumReader<T> genericReader))
                {
                    // GetSchemaAsync may throw
                    string writerSchemaJson = SchemaRegisterClient.GetSchemaAsync(writerId).Result;
                    writerSchema = Avro.Schema.Parse(writerSchemaJson);

                    // can be of multiple type: Record, Primitive...
                    // we don't read against a given schema, so writer and reader schema are same
                    genericReader = new SpecificReader<T>(writerSchema, ReaderSchema ?? writerSchema);
                    readerBySchemaId[writerId] = genericReader;
                }
                else
                {
                    writerSchema = genericReader.ReaderSchema;
                }

                return genericReader.Read(default(T), new BinaryDecoder(stream));
            }
        }
        
        T IDeserializer<T>.Deserialize(string topic, byte[] array)
        {
            return Deserialize(topic, array, out Avro.Schema schema, out int writerId);
        }
        
        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
        {
            return config;
        }
    }
}
