using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Avro.Specific;
using Confluent.Kafka;

namespace Confluent.SchemaRegistry.Serdes.Avro
{
    public class MultiSchemaAvroDeserializer : IAsyncDeserializer<ISpecificRecord>
    {
        private readonly Dictionary<string, IAsyncDeserializer<ISpecificRecord>> deserializersBySchemaName;
        private readonly ConcurrentDictionary<int, IAsyncDeserializer<ISpecificRecord>> deserializersBySchemaId;
        private readonly ISchemaRegistryClient schemaRegistryClient;

        public MultiSchemaAvroDeserializer(ISchemaRegistryClient schemaRegistryClient, AvroDeserializerConfig avroDeserializerConfig = null)
            : this(schemaRegistryClient,
                  AppDomain.CurrentDomain
                  .GetAssemblies()
                  .SelectMany(a => a.GetTypes())
                  .Where(t => t.IsClass && !t.IsAbstract && typeof(ISpecificRecord).IsAssignableFrom(t) && GetSchema(t) != null),
                   avroDeserializerConfig,
                   checkTypes: false)
        {
        }

        public MultiSchemaAvroDeserializer(Func<IEnumerable<Type>> typeProvider, ISchemaRegistryClient schemaRegistryClient, AvroDeserializerConfig avroDeserializerConfig = null)
            : this(schemaRegistryClient,
                  (typeProvider ?? throw new ArgumentNullException(nameof(typeProvider)))(),
                   avroDeserializerConfig)
        {
        }

        public MultiSchemaAvroDeserializer(IEnumerable<Type> types, ISchemaRegistryClient schemaRegistryClient, AvroDeserializerConfig avroDeserializerConfig = null)
            :this(schemaRegistryClient, types, avroDeserializerConfig)
        {   
        }

        private MultiSchemaAvroDeserializer(ISchemaRegistryClient schemaRegistryClient, IEnumerable<Type> types, AvroDeserializerConfig avroDeserializerConfig, bool checkTypes = true)
        {
            this.schemaRegistryClient = schemaRegistryClient ?? throw new ArgumentNullException(nameof(schemaRegistryClient));

            var typeArray = (types ?? throw new ArgumentNullException(nameof(types))).ToArray();

            if (typeArray.Length == 0)
            {
                throw new ArgumentException("Type collection must contain at least one item.", nameof(types));
            }

            if (checkTypes && !typeArray.All(t => t.IsClass && !t.IsAbstract && typeof(ISpecificRecord).IsAssignableFrom(t) && GetSchema(t) != null))
            {
                throw new ArgumentOutOfRangeException(nameof(types));
            }

            deserializersBySchemaId = new ConcurrentDictionary<int, IAsyncDeserializer<ISpecificRecord>>();
            deserializersBySchemaName = typeArray.ToDictionary(t => GetSchema(t).Fullname, t => CreateDeserializer(t, avroDeserializerConfig));
        }

        public async Task<ISpecificRecord> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            var deserializer = await GetDeserializer(data);

            return deserializer == null ? null : await deserializer.DeserializeAsync(data, isNull, context);
        }

        private IAsyncDeserializer<ISpecificRecord> CreateDeserializer(Type specificType, AvroDeserializerConfig avroDeserializerConfig)
        {
            var constructedDeserializerType = typeof(DeserializerWrapper<>).MakeGenericType(specificType);
            return (IAsyncDeserializer<ISpecificRecord>)Activator.CreateInstance(constructedDeserializerType, schemaRegistryClient, avroDeserializerConfig);
        }

        private async Task<IAsyncDeserializer<ISpecificRecord>> GetDeserializer(ReadOnlyMemory<byte> data)
        {
            if (data.Length < 5)
            {
                throw new InvalidDataException($"Expecting data framing of length 5 bytes or more but total data size is {data.Length} bytes");
            }

            var schemaId = GetSchemaId(data);

            if (deserializersBySchemaId.TryGetValue(schemaId, out var deserializer))
            {
                return deserializer;
            }

            var confluentSchema = await schemaRegistryClient.GetSchemaAsync(schemaId);
            var avroSchema = global::Avro.Schema.Parse(confluentSchema.SchemaString);

            _ = deserializersBySchemaName.TryGetValue(avroSchema.Fullname, out deserializer);
            _ = deserializersBySchemaId.TryAdd(schemaId, deserializer);

            return deserializer;
        }

        private static global::Avro.Schema GetSchema(IReflect type)
        {
            return (global::Avro.Schema)type.GetField("_SCHEMA", BindingFlags.Public | BindingFlags.Static)?.GetValue(null);
        }

        private static int GetSchemaId(ReadOnlyMemory<byte> data)
        {
            var firstFiveBytes = data.Span.Slice(0, 5);

            if (firstFiveBytes[0] != Constants.MagicByte)
            {
                throw new InvalidDataException($"Expecting data with Confluent Schema Registry framing. Magic byte was {firstFiveBytes[0]}, expecting 0");
            }

            var schemaId = BinaryPrimitives.ReadInt32BigEndian(firstFiveBytes.Slice(1));

            return schemaId;
        }

        private class DeserializerWrapper<T> : IAsyncDeserializer<ISpecificRecord> where T: ISpecificRecord
        {
            private readonly AvroDeserializer<T> avroDeserializer;

            public DeserializerWrapper(ISchemaRegistryClient schemaRegistryClient, AvroDeserializerConfig avroDeserializerConfig = null)
            {
                avroDeserializer = new AvroDeserializer<T>(schemaRegistryClient, avroDeserializerConfig);
            }

            public async Task<ISpecificRecord> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
            {
                return await avroDeserializer.DeserializeAsync(data, isNull, context);
            }
        }
    }
}
