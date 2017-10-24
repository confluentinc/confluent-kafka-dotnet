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

//using System;
//using System.Collections.Generic;
//using System.Dynamic;
//using System.IO;
//using System.Net;
//using System.Runtime.ExceptionServices;
//using System.Threading.Tasks;
//using Avro.Generic;
//using Avro.IO;
//using Confluent.Kafka.Serialization;

//namespace Confluent.Kafka.SchemaRegistry.Serializer
//{
//    /// <summary>
//    /// Dynamic object for serialization to a kafka topic
//    /// </summary>
//    public class AvroProduceRecord : DynamicObject
//    {
//        /// <summary>
//        /// Schema registry subject (with key/value suffix)
//        /// </summary>
//        public string Subject { get; }

//        /// <summary>
//        /// Schema Id corresponding to the serializer
//        /// </summary>
//        public int Id { get; }

//        private GenericWriter<object> _genericSerializer;

//        private dynamic _avroRecord;

//        public AvroProduceRecord(GenericWriter<object> genericSerializer, string topic, int id)
//        {
//            _genericSerializer = genericSerializer;
//            Subject = topic;
//            Id = id;
//            if (genericSerializer.Schema is Avro.RecordSchema)
//            {
//                _avroRecord = new GenericRecord(genericSerializer.Schema as Avro.RecordSchema);
//            }
//            else if (genericSerializer.Schema is Avro.FixedSchema)
//            {
//                _avroRecord = new GenericFixed(genericSerializer.Schema as Avro.FixedSchema);
//            }
//            else if(genericSerializer.Schema is Avro.EnumSchema)
//            {
//                _avroRecord = new GenericEnum(genericSerializer.Schema as Avro.EnumSchema, (genericSerializer.Schema as Avro.EnumSchema).Symbols[0]);
//            }
//        }

//        public object this[string name]
//        {
//            get { return _avroRecord[name]; }
//            set { _avroRecord.Add(name, value); }
//        }

//        /// <summary>
//        /// to use for type which are not record
//        /// </summary>
//        public object Value
//        {
//            get { return _avroRecord; }
//            set { _avroRecord =  value; }
//        }

//        public Avro.Schema Schema => _genericSerializer.Schema;

//        public void SerializeAvroTo(Stream stream)
//        {
//            _genericSerializer.Write(_avroRecord, new BinaryEncoder(stream));
//        }

//        /// <summary>
//        /// Provides the implementation for operations that get member values. Classes derived from the <see cref="T:System.Dynamic.DynamicObject" /> class can override this method to specify dynamic behavior for operations such as getting a value for a property.
//        /// </summary>
//        /// <param name="binder">Provides information about the object that called the dynamic operation. The binder.Name property provides the name of the member on which the dynamic operation is performed. For example, for the Console.WriteLine(sampleObject.SampleProperty) statement, where sampleObject is an instance of the class derived from the <see cref="T:System.Dynamic.DynamicObject" /> class, binder.Name returns "SampleProperty". The binder.IgnoreCase property specifies whether the member name is case-sensitive.</param>
//        /// <param name="result">The result of the get operation. For example, if the method is called for a property, you can assign the property value to <paramref name="result" />.</param>
//        /// <returns>
//        /// True if the operation is successful; otherwise, false. If this method returns false, the run-time binder of the language determines the behavior. (In most cases, a run-time exception is thrown.).
//        /// </returns>
//        /// <exception cref="System.ArgumentNullException">Thrown when <paramref name="binder"/> is null.</exception>
//        public override bool TryGetMember(
//            GetMemberBinder binder,
//            out object result)
//        {
//            return _avroRecord.TryGetMember(binder, out result);
//        }

//        /// <summary>
//        /// Provides the implementation for operations that set member values. Classes derived from the <see cref="T:System.Dynamic.DynamicObject" /> class can override this method to specify dynamic behavior for operations such as setting a value for a property.
//        /// </summary>
//        /// <param name="binder">Provides information about the object that called the dynamic operation. The binder.Name property provides the name of the member to which the value is being assigned. For example, for the statement sampleObject.SampleProperty = "Test", where sampleObject is an instance of the class derived from the <see cref="T:System.Dynamic.DynamicObject" /> class, binder.Name returns "SampleProperty". The binder.IgnoreCase property specifies whether the member name is case-sensitive.</param>
//        /// <param name="value">The value to set to the member. For example, for sampleObject.SampleProperty = "Test", where sampleObject is an instance of the class derived from the <see cref="T:System.Dynamic.DynamicObject" /> class, the <paramref name="value" /> is "Test".</param>
//        /// <returns>
//        /// True if the operation is successful; otherwise, false. If this method returns false, the run-time binder of the language determines the behavior. (In most cases, a language-specific run-time exception is thrown.).
//        /// </returns>
//        /// <exception cref="System.ArgumentNullException">Thrown when <paramref name="binder"/> is null.</exception>
//        public override bool TrySetMember(SetMemberBinder binder, object value)
//        {
//            return _avroRecord.TrySetMember(binder, value);
//        }
//    }

//    /// <summary>
//    /// Avro serializer/deserializer, use dynamic AvroRecord object
//    /// </summary>
//    public class KafkaGenericAvroSerializer : IDeserializer<object>, ISerializer<object>
//    {
//        //AvroProduceRecord for seialization so we don't have to ask multiple times the registry,
//        //Avro record for reading as we always will ask registry for id check

//        //We use the same format as confluentinc java implementation for compatibility :

//        // [0] : Magic byte (0 as of today, used for future version with breaking change)
//        // [1-4] : unique global id of avro schema used for write (as registered in schema registry), BIG ENDIAN
//        // following: data serialized with corresponding schema

//        //topic refer to kafka topic
//        //subject refers to schema registry subject. Usually topic postfixed by -key or -data

//        public const byte MAGIC_BYTE = 0;

//        private readonly Confluent.Kafka.SchemaRegistry.ISchemaRegistryClient _schemaRegisterRest;

//        // Deserializers against different versions of the schema (older or newer)
//        private readonly Dictionary<int, GenericReader<object>> _avroSerializerBySchemaId = new Dictionary<int, GenericReader<object>>();
//        private readonly Dictionary<string, int> _avroIdBySchema = new Dictionary<string, int>();
//        private readonly Dictionary<string, GenericWriter<object>> _avroSerializerBySchema = new Dictionary<string, GenericWriter<object>>();

//        private bool isKey;

//        /// <summary>
//        /// Initiliaze an avro serializer.
//        /// </summary>
//        /// <param name="schemaRegisterRest"></param>
//        /// <param name="settings">Avro settings (posix time or iso date...)</param>
//        public KafkaGenericAvroSerializer(Confluent.Kafka.SchemaRegistry.ISchemaRegistryClient schemaRegisterRest, bool isKey)
//        {
//            _schemaRegisterRest = schemaRegisterRest;
//            this.isKey = isKey;
//        }

//        /// <summary>
//        ///
//        /// </summary>
//        /// <param name="subject">schema registry subject (with -key / -value suffix usually)</param>
//        /// <returns></returns>
//        private async Task<GenericWriter<object>> GetSerializerAsync(string subject, string writerSchema)
//        {
//            if (!_avroSerializerBySchema.TryGetValue(subject, out GenericWriter<object> serializer))
//            {
//                int id = await _schemaRegisterRest.RegisterAsync(subject, writerSchema).ConfigureAwait(false);
//                serializer = new GenericWriter<object>(Avro.Schema.Parse(writerSchema));
//                _avroSerializerBySchema[subject] = serializer;
//                //TODO _avroSerializerBySchemaId[id] = serializer;
//                _avroIdBySchema[writerSchema] = id;
//            }
//            return serializer;
//        }

//        /// <summary>
//        /// Return an aobject which can be populated given schema of a topic
//        /// You can reuse an object after it has been serialized to serialize new value
//        /// </summary>
//        /// <param name="topic">kakfka topic</param>
//        /// <param name="writerSchema">avro schema used for serialization</param>
//        /// <param name="isKey"></param>
//        /// <returns></returns>
//        public async Task<AvroProduceRecord> GenerateRecordAsync(string topic, string writerSchema, bool isKey = false)
//        {
//            var subject = _schemaRegisterRest.GetRegistrySubject(topic, isKey);
//            var genericSerializer = await GetSerializerAsync(subject, writerSchema).ConfigureAwait(false);
//            int id = _avroIdBySchema[writerSchema];
//            return new AvroProduceRecord(genericSerializer, subject, id);
//        }

//        public void Serialize(Stream stream, AvroProduceRecord record)
//        {
//            //1 byte: magic byte
//            stream.WriteByte(MAGIC_BYTE);

//            //4 bytes: schema global unique id
//            //use network order to b compatible with other implementation
//            byte[] idBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(record.Id));
//            stream.Write(idBytes, 0, 4);

//            //avro message
//            record.SerializeAvroTo(stream);
//        }

//        /// <summary>
//        /// If SchemaId is not initialized, may throw SchemaRegistry related exception
//        /// </summary>
//        /// <param name="stream"></param>
//        /// <returns>an AvroRecord or a primitive type given the schema</returns>
//        /// <exception cref="InvalidDataException">Magic byte is not 0</exception>
//        /// <exception cref="System.Runtime.Serialization.SerializationException">Schemas do not match</exception>
//        public object Deserialize(Stream stream)
//        {
//            using (var reader = new BinaryReader(stream))
//            {
//                int magicByte = reader.ReadByte();
//                if (magicByte != MAGIC_BYTE)
//                {
//                    //may change in the future with new format
//                    throw new InvalidDataException("magic byte should be 0");
//                }
//                //Data may have been serialized with an other version of the schema (new fields, aliases...)
//                var writerId = reader.ReadInt32();
//                writerId = IPAddress.NetworkToHostOrder(writerId);
//                if (!_avroSerializerBySchemaId.TryGetValue(writerId, out GenericReader<object> seralizer))
//                {
//                    try
//                    {
//                        string writerSchema = _schemaRegisterRest.GetSchemaAsync(writerId).Result;
//                        var schema = Avro.Schema.Parse(writerSchema);
//                        seralizer = new GenericReader<object>(schema, schema);
//                        _avroSerializerBySchemaId[writerId] = seralizer;
//                    }
//                    catch (AggregateException ae)
//                    {
//                        //could not create the deserializer, schema is not compatible, or couldn't contact schema registry

//                        //for GetSchemaAsync.Result
//                        //can't make this method async
//                        //and don't want to throw AggregateException, juste the InnerException, with the proper stack trace
//                        ExceptionDispatchInfo.Capture(ae.InnerException).Throw();
//                        throw; //not used, just for analysers
//                    }
//                }
//                object obj;
//                return seralizer.Read(seralizer, new BinaryDecoder(stream));
//            }
//        }

//        /// <summary>
//        /// If SchemaId is not initialized, may throw SchemaRegistry related exception
//        /// </summary>
//        /// <param name="stream"></param>
//        /// <returns>an AvroRecord or a primitive type given the schema</returns>
//        /// <exception cref="InvalidDataException">Magic byte is not 0</exception>
//        /// <exception cref="System.Runtime.Serialization.SerializationException">Schemas do not match</exception>
//        public async Task<object> DeserializeAsync(Stream stream)
//        {
//            using (BinaryReader reader = new BinaryReader(stream))
//            {
//                int magicByte = reader.ReadByte();
//                if (magicByte != MAGIC_BYTE)
//                {
//                    //may change in the future with new format
//                    throw new InvalidDataException("magic byte should be 0");
//                }
//                //Data may have been serialized with an other version of the schema (new fields, aliases...)
//                var writerId = reader.ReadInt32();
//                writerId = IPAddress.NetworkToHostOrder(writerId);
//                if (!_avroSerializerBySchemaId.TryGetValue(writerId, out GenericReader<object> deserializer))
//                {
//                    string writerSchema = await _schemaRegisterRest.GetSchemaAsync(writerId).ConfigureAwait(false);
//                    var schema = Avro.Schema.Parse(writerSchema);
//                    deserializer = new GenericReader<object>(schema, schema);
//                    _avroSerializerBySchemaId[writerId] = deserializer;
//                }
//                return deserializer.Read(deserializer, new BinaryDecoder(stream));
//            }
//        }

//        /// <summary>
//        /// Deserialize array to given tip
//        /// </summary>
//        /// <param name="array"></param>
//        /// <param name="topic">kafka topic, not used for avro deserialization</param>
//        /// <param name="isKey">key or not, not used for deserialization</param>
//        /// <returns>an AvroRecord or a primitive type given the schema</returns>
//        /// <exception cref="System.Runtime.Serialization.SerializationException">Schemas do not match</exception>
//        public object Deserialize(string topic, byte[] array)
//        {
//            //topic not necessary for deserialization (knowing if it's key or not neither)
//            using (var stream = new MemoryStream(array))
//            {
//                return Deserialize(stream);
//            }
//        }
//        /// <summary>
//        /// Serializer an <see cref="AvroProduceRecord"/>
//        /// </summary>
//        /// <param name="data"></param>
//        /// <param name="length"></param>
//        /// <returns></returns>
//        public byte[] Serialize(AvroProduceRecord data, out int length)
//        {
//            using (var stream = new MemoryStream(30))
//            {
//                Serialize(stream, data);
//                length = (int)stream.Length;
//                //we return the buffer of the stream and not a copy to try performing zero copy operation
//#if NET451
//                return stream.GetBuffer();
//#elif NETSTANDARD1_3
//                ArraySegment<byte> buffer; //offset is always 0 here
//                return (stream.TryGetBuffer(out buffer) ? buffer.Array : stream.ToArray());
//#else
//                //old way - copy array instead of reusing the generated one
//                return stream.ToArray();
//#endif
//            }
//        }

//        /// <summary>
//        /// Serializer an <see cref="AvroProduceRecord"/>
//        /// </summary>
//        /// <param name="data"></param>
//        /// <param name="length"></param>
//        /// <returns></returns>
//        public byte[] Serialize(string topic, object data)
//        {
//            using (var stream = new MemoryStream(30))
//            {
//                Serialize(stream, data);
//                length = (int)stream.Length;
//                //we return the buffer of the stream and not a copy to try performing zero copy operation
//#if NET451
//                return stream.GetBuffer();
//#elif NETSTANDARD1_3
//                ArraySegment<byte> buffer; //offset is always 0 here
//                return (stream.TryGetBuffer(out buffer) ? buffer.Array : stream.ToArray());
//#else
//                //old way - copy array instead of reusing the generated one
//                return stream.ToArray();
//#endif
//            }
//        }

//        /// <summary>
//        ///
//        /// </summary>
//        /// <param name="data"></param>
//        /// <param name="topic">not use</param>
//        /// <param name="length"></param>
//        /// <param name="isKey"></param>
//        /// <returns></returns>
//        public byte[] Serialize(AvroProduceRecord data, string topic, out int length, bool isKey = false)
//        {
//            if (_schemaRegisterRest.GetRegistrySubject(topic, isKey) != data.Subject)
//            {
//                throw new ArgumentException($"{nameof(topic)} {topic} does not match {nameof(data)} {data.Subject}");
//            }
//            return Serialize(data, out length);
//        }

//        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
//        {
//            throw new NotImplementedException();
//        }
//    }
//}
