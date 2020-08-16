using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Avro.Generic;
using Avro.IO;

namespace Confluent.SchemaRegistry.Serdes
{
    internal abstract class DeserializerImplBase<T> : IAvroDeserializerImpl<T>
    {
        /// <remarks>
        ///     A datum reader cache (one corresponding to each write schema that's been seen)
        ///     is maintained so that they only need to be constructed once.
        /// </remarks>
        private readonly Dictionary<int, DatumReader<T>> datumReaderBySchemaId 
            = new Dictionary<int, DatumReader<T>>();
       
        private readonly SemaphoreSlim deserializeMutex = new SemaphoreSlim(1);

        private readonly ISchemaRegistryClient schemaRegistryClient;

        public DeserializerImplBase(ISchemaRegistryClient schemaRegistryClient)
        {
            this.schemaRegistryClient = schemaRegistryClient ?? throw new ArgumentNullException(nameof(schemaRegistryClient));
        }

        public async Task<T> Deserialize(string topic, byte[] array)
        {
            try
            {
                // Note: topic is not necessary for deserialization (or knowing if it's a key 
                // or value) only the schema id is needed.

                if (array.Length < 5)
                {
                    throw new InvalidDataException($"Expecting data framing of length 5 bytes or more but total data size is {array.Length} bytes");
                }

                using (var stream = new MemoryStream(array))
                using (var reader = new BinaryReader(stream))
                {
                    var magicByte = reader.ReadByte();
                    if (magicByte != Constants.MagicByte)
                    {
                        throw new InvalidDataException($"Expecting data with Confluent Schema Registry framing. Magic byte was {array[0]}, expecting {Constants.MagicByte}");
                    }
                    var writerId = IPAddress.NetworkToHostOrder(reader.ReadInt32());

                    DatumReader<T> datumReader;
                    await deserializeMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
                    try
                    {
                        datumReaderBySchemaId.TryGetValue(writerId, out datumReader);
                        if (datumReader == null)
                        {
                            // TODO: If any of this cache fills up, this is probably an
                            // indication of misuse of the deserializer. Ideally we would do 
                            // something more sophisticated than the below + not allow 
                            // the misuse to keep happening without warning.
                            if (datumReaderBySchemaId.Count > schemaRegistryClient.MaxCachedSchemas)
                            {
                                datumReaderBySchemaId.Clear();
                            }

                            var writerSchemaResult = await schemaRegistryClient.GetSchemaAsync(writerId).ConfigureAwait(continueOnCapturedContext: false);
                            if (writerSchemaResult.SchemaType != SchemaType.Avro)
                            {
                                throw new InvalidOperationException("Expecting writer schema to have type Avro, not {writerSchemaResult.SchemaType}");
                            }
                            var writerSchema = Avro.Schema.Parse(writerSchemaResult.SchemaString);

                            datumReader = CreateDatumReader(writerSchema);
                            datumReaderBySchemaId[writerId] = datumReader;
                        }
                    }
                    finally
                    {
                        deserializeMutex.Release();
                    }
                    
                    return datumReader.Read(default(T), new BinaryDecoder(stream));
                }
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        protected abstract DatumReader<T> CreateDatumReader(Avro.Schema writerSchema);
    }
}