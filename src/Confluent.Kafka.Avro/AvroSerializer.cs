using System;
using System.Linq;
using System.Collections.Generic;
using Confluent.SchemaRegistry;
using Avro.Generic;
using Avro.IO;
using Confluent.Kafka;
using System.IO;
using System.Net;


namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     Avro generic serializer.
    /// </summary>
    /// <remarks>
    ///     Serialization format:
    ///       byte 0:           Magic byte use to identify the protocol format.
    ///       bytes 1-4:        Unique global id of the avro schema that was used for encoding (as registered in Confluent Schema Registry), big endian.
    ///       following bytes:  The serialized data.
    /// </remarks>
    public class AvroSerializer : ISerializer<GenericRecord>
    {
        /// <summary>
        ///	    The ISchemaRegistryClient instance used for communication
        ///	    with Confluent Schema Registry.
        /// </summary>
        public ISchemaRegistryClient SchemaRegistryClient { get; private set; }

        /// <summary>
        ///	    True if this serializer is used for serializing Kafka message keys,
        ///	    false if it is used for serializing Kafka message values.
        /// </summary>
        public bool IsKey { get; private set; }

        private bool disposeClientOnDispose;

        /// <summary>
        ///     The default initial size (in bytes) of buffers used for message 
        ///     serialization.
        /// </summary>
        public const int DefaultInitialBufferSize = 128;

        /// <summary>
        ///     True if the serializer will attempt to auto-register un-recognized schemas
        ///     with Confluent Schema Registry, false if not.
        /// </summary>
        public bool AutoRegisterSchema { get; private set; } = true;

        /// <summary>
        ///     Initial size (in bytes) of the buffer used for message serialization.
        /// </summary>
        /// <remarks>
        ///     Use a value high enough to avoid resizing of buffer, but small enough
        ///     to avoid excessive memory use.
        /// </remarks>
        public int InitialBufferSize { get; private set; } = DefaultInitialBufferSize;

        public AvroSerializer()
        {

        }

        public AvroSerializer(ISchemaRegistryClient schemaRegistryClient)
        {
            disposeClientOnDispose = false;
            SchemaRegistryClient = schemaRegistryClient;
        }

        public void Dispose()
        {
            if (disposeClientOnDispose)
            {
                SchemaRegistryClient.Dispose();
            }
        }

        private Dictionary<Avro.RecordSchema, string> knownSchemas = new Dictionary<Avro.RecordSchema, string>();

        private HashSet<KeyValuePair<string, string>> registeredSchemas = new HashSet<KeyValuePair<string, string>>();

        private Dictionary<string, int> schemaStrings = new Dictionary<string, int>();


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
            // TODO: If any of these caches fills up, this is probably an
            // indication of misuse. Ideally we would do something more 
            // sophisticated than this & not allow the misuse to keep 
            // happening without warning.
            if (knownSchemas.Count > SchemaRegistryClient.MaxCachedSchemas ||
                registeredSchemas.Count > SchemaRegistryClient.MaxCachedSchemas ||
                schemaStrings.Count > SchemaRegistryClient.MaxCachedSchemas)
            {
                knownSchemas.Clear();
                registeredSchemas.Clear();
                schemaStrings.Clear();
            }

            var writerSchema = data.Schema;
            string writerSchemaString = null;

            // Note: The default object hash used here is cheap - this is a quick
            // lookup from writer schema object reference -> corresponding schema 
            // string instance under expected usage.
            if (knownSchemas.ContainsKey(writerSchema))
            {
                writerSchemaString = knownSchemas[writerSchema];
            }
            else
            {
                // Note: Even if the schema string used to construct two 
                // GenericRecord instances is different, if they are represent
                // the same schema, the output of ToString() will be identical.
                writerSchemaString = writerSchema.ToString();
                knownSchemas.Add(writerSchema, writerSchemaString);
            }

            string subject = IsKey
                ? SchemaRegistryClient.ConstructKeySubjectName(topic)
                : SchemaRegistryClient.ConstructValueSubjectName(topic);

            // TODO: Optimize this lookup with a custom hash function that
            // uses the writerSchemaString reference, not value.
            var subjectSchemaPair = new KeyValuePair<string, string>(subject, writerSchemaString);
            if (!registeredSchemas.Contains(subjectSchemaPair))
            {
                // first usage: register/get schema to check compatibility
                if (AutoRegisterSchema)
                {
                    schemaStrings.Add(
                        writerSchemaString, 
                        SchemaRegistryClient.RegisterSchemaAsync(subject, writerSchemaString).Result
                    );
                }
                else
                {
                    schemaStrings.Add(
                        writerSchemaString, 
                        SchemaRegistryClient.GetSchemaIdAsync(subject, writerSchemaString).Result
                    );
                }

                registeredSchemas.Add(subjectSchemaPair);
            }

            var schemaId = schemaStrings[writerSchemaString];

            using (var stream = new MemoryStream(InitialBufferSize))
            using (var writer = new BinaryWriter(stream))
            {
                stream.WriteByte(Constants.MagicByte);
                writer.Write(IPAddress.HostToNetworkOrder(schemaId));
                new GenericWriter<GenericRecord>(writerSchema)
                    .Write(data, new BinaryEncoder(stream));
                return stream.ToArray();
            }
        }

        /// <include file='../Confluent.Kafka/include_docs.xml' path='API/Member[@name="ISerializer_Configure"]/*' />
        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
        {
            var keyOrValue = isKey ? "Key" : "Value";
            var srConfig = config.Where(item => item.Key.StartsWith("schema.registry."));
            var avroConfig = config.Where(item => item.Key.StartsWith("avro."));
            IsKey = isKey;

            if (avroConfig.Count() != 0)
            {
                int? initialBufferSize = (int?)Utils.ExtractPropertyValue(config, isKey, Constants.InitialBufferSizePropertyName, "AvroSerializer", typeof(int));
                if (initialBufferSize != null) { InitialBufferSize = initialBufferSize.Value; }

                bool? autoRegisterSchema = (bool?)Utils.ExtractPropertyValue(config, isKey, Constants.AutoRegisterSchemaPropertyName, "AvroSerializer", typeof(bool));
                if (autoRegisterSchema != null) { AutoRegisterSchema = autoRegisterSchema.Value; }

                foreach (var property in avroConfig)
                {
                    if (property.Key != Constants.AutoRegisterSchemaPropertyName && property.Key != Constants.InitialBufferSizePropertyName)
                    {
                        throw new ArgumentException($"{keyOrValue} AvroSerializer: unexpected configuration parameter {property.Key}");
                    }
                }
            }

            if (srConfig.Count() != 0)
            {
                if (SchemaRegistryClient != null)
                {
                    throw new ArgumentException($"{keyOrValue} AvroSerializer schema registry client was configured via both the constructor and configuration parameters.");
                }
 
                disposeClientOnDispose = true;
                SchemaRegistryClient = new CachedSchemaRegistryClient(config);
            }

            if (SchemaRegistryClient == null)
            {
                throw new ArgumentException($"{keyOrValue} AvroSerializer schema registry client was not supplied or configured.");
            }

            return config.Where(item => !item.Key.StartsWith("schema.registry.") && !item.Key.StartsWith("avro."));
        }

    }
}