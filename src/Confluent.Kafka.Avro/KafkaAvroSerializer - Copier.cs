//using Avro;
//using System;
//using System.Collections.Generic;
//using System.IO;
//using System.Net;
//using System.Runtime.ExceptionServices;
//using System.Threading.Tasks;
//using Confluent.Kafka.SchemaRegistry;
//using Confluent.Kafka.SchemaRegistry.Rest.Exceptions;
//using Confluent.Kafka.Serialization;
//using Avro.Specific;
//using Avro.IO;
//using System.Runtime.Serialization;

//namespace Confluent.Kafka.SchemaRegistry.Serializer
//{
//    public class AvroSerializer<T> : ISerializer<T>
//    {
//        //We use the same format as confluentinc java implementation for compatibility :

//        // [0] : Magic byte (0 as of today, used for future version with breaking change)
//        // [1-4] : unique global id of avro schema used for write (as registered in schema registry), BIG ENDIAN
//        // following: data serialized with corresponding schema

//        //topic refer to kafka topic
//        //subject refers to schema registry subject. Usually topi postfixed by -key or -data

//        public const byte MAGIC_BYTE = 0;

//        private bool isKey;
//        private readonly SpecificWriter<T> _avroSerializer;
//        private readonly ISchemaRegistryClient _schemaRegisterRest;

//        // Deserializers against different versions of the schema (older or newer)
//        private readonly Dictionary<int, SpecificReader<T>> _avroDeserializerBySchemaId = new Dictionary<int, SpecificReader<T>>();

//        /// <summary>
//        /// Avro schema generated for this class
//        /// </summary>
//        public string Schema { get; }

//        /// <summary>
//        /// Initiliaze an avro serializer.
//        /// </summary>
//        /// <param name="schemaRegisterRest"></param>
//        /// <param name="settings">Avro settings. Use posixTime by default</param>
//        public KafkaAvroSerializer(ISchemaRegistryClient schemaRegisterRest, bool isKey)
//        {
//            Avro.Schema schema;
//            if(typeof(T).Equals(typeof(int)))
//            {
//                schema = Avro.Schema.Parse(Avro.Schema.GetTypeString(Avro.Schema.Type.Int));
//            }
//            else if(typeof(T).Equals(typeof(bool)))
//            {
//                schema = Avro.Schema.Parse(Avro.Schema.GetTypeString(Avro.Schema.Type.Boolean));
//            }
//            else if (typeof(T).Equals(typeof(ISpecificRecord)))
//            {
//                throw new Exception("You must use a concrete type");
//            }
//            _avroSerializer = new SpecificWriter<T>(new T().Schema);
//            _schemaRegisterRest = schemaRegisterRest;
//            this.isKey = isKey;

//            Schema = _avroSerializer.Schema.ToString();
//        }

//        /// <summary>
//        /// Serialize data with zero copy
//        /// If SchemaId is not initialized, may throw SchemaRegistry related exception
//        /// </summary>
//        /// <param name="data"></param>
//        /// <param name="topic"></param>
//        /// <param name="length">Length of the result to take into account </param>
//        /// <param name="isKey"></param>
//        /// <returns></returns>

//        public byte[] Serialize(string topic, T data)
//        {
//            //TODO check to use something else than 30 which is not optimal
//            //best would be a dynamic size depending from 99th percentile of generated array for example
//            using (var stream = new MemoryStream(30))
//            {
//                SerializeToStream(stream, data, _schemaRegisterRest.GetRegistrySubject(topic, isKey));
//                //we return the buffer of the stream and not a copy to try performing zero copy operation
//#if NET451
//                //would be way better to avoid a copy - return a buffer with the length
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
//        /// Serialize data to avro message preceeded by magicbyte and schemaId
//        /// If SchemaId is not initialized, may throw SchemaRegistry related exception
//        /// </summary>
//        /// <param name="stream"></param>
//        /// <param name="data"></param>
//        /// <param name="subject"></param>
//        public void SerializeToStream(Stream stream, T data, string subject)
//        {
//            try
//            {
//                //TODO would be nice to have async code only here
//                SerializeAsync(stream, data, subject).Wait();
//            }
//            catch (AggregateException ae)
//            {
//                //rethrow inner exception without losing stack (because of async)
//                ExceptionDispatchInfo.Capture(ae.InnerException).Throw();
//            }
//        }

//        /// <summary>
//        /// Serialize data to avro message preceeded by magicbyte and schemaId
//        /// If SchemaId is not initialized, may throw SchemaRegistry related exception
//        /// </summary>
//        /// <param name="stream"></param>
//        /// <param name="data"></param>
//        /// <param name="subject"></param>
//        public async Task SerializeAsync(Stream stream, T data, string subject)
//        {
//            int schemaId;
//            try
//            {
//                //we need to be sure schema is registred under a subject so even if we know schemaId, need to register
//                //might check if caching last subject here is more efficient
//                schemaId = await _schemaRegisterRest.RegisterAsync(subject, Schema).ConfigureAwait(false);
//            }
//            catch (SchemaRegistryInternalException e) when (e.ErrorCode == 409) //TODO use enum for errorcode
//            {
//                throw new SerializationException($"schema {Schema} is incompatible with subject {subject}", e);
//            }
//            catch (Exception e)
//            {
//                throw new SerializationException($"Error during schema registration", e);
//            }

//            //1 byte: magic byte
//            stream.WriteByte(MAGIC_BYTE);

//            //4 bytes: schema global unique id
//            //use network order to b compatible with other implementation
//            byte[] idBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(schemaId));
//            stream.Write(idBytes, 0, 4);

//            //avro message
//            _avroSerializer.Write(data, new BinaryEncoder(stream));
//        }
        
//        /// <summary>
//        /// Deserialize array to given type
//        /// </summary>
//        /// <param name="array"></param>
//        /// <param name="topic">kafka topic, not used for avro deserialization</param>
//        /// <returns></returns>
//        /// <exception cref="System.Runtime.Serialization.SerializationException">Schemas do not match</exception>
//        public T Deserialize(string topic, byte[] array)
//        {
//            //topic not necessary for deserialization (knowing if it's key or not neither)
//            using (var stream = new MemoryStream(array))
//            {
//                return DeserializeStream(stream);
//            }
//        }

//        /// <summary>
//        /// If SchemaId is not initialized, may throw SchemaRegistry related exception
//        /// </summary>
//        /// <param name="stream"></param>
//        /// <returns></returns>
//        /// <exception cref="InvalidDataException">Magic byte is not 0</exception>
//        /// <exception cref="System.Runtime.Serialization.SerializationException">Schemas do not match</exception>
//        public T DeserializeStream(Stream stream)
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
//                int writerId = reader.ReadInt32();
//                writerId = IPAddress.NetworkToHostOrder(writerId);
//                if (!_avroDeserializerBySchemaId.TryGetValue(writerId, out SpecificReader<T> deseralizer))
//                {
//                    try
//                    {
//                        string writerJsonSchema = _schemaRegisterRest.GetSchemaAsync(writerId).Result;
//                        var writerSchema = Avro.Schema.Parse(writerJsonSchema);
//                        deseralizer = new SpecificReader<T>(writerSchema, _avroSerializer.Schema);
//                        _avroDeserializerBySchemaId[writerId] = deseralizer;
//                    }
//                    catch (AggregateException ae)
//                    {
//                        //could not create the deserializer, schema is incompatible or couldn't contact schema registry

//                        //for GetSchemaAsync.Result
//                        //can't make the current method async /and don't want to throw AggregateException,
//                        //juste the InnerException, with the proper stack trace
//                        ExceptionDispatchInfo.Capture(ae.InnerException).Throw();
//                        throw; //not used, just for analysers
//                    }
//                }
//                var result = new T();
//                return deseralizer.Read(result, new BinaryDecoder(stream));
//            }
//        }

//        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
//        {
//            throw new NotImplementedException();
//        }
//    }
//}
