using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Avro.Specific;
using Confluent.Kafka;

namespace Confluent.SchemaRegistry.Serdes.Avro
{
    public class MultiSchemaAvroDeserializer : IAsyncDeserializer<ISpecificRecord>
    {
        private readonly Dictionary<string, Func<ReadOnlyMemory<byte>, bool, SerializationContext, Task<ISpecificRecord>>> deserializersBySchema;
        private readonly ConcurrentDictionary<int, Func<ReadOnlyMemory<byte>, bool, SerializationContext, Task<ISpecificRecord>>> deserializersBySchemaId;
        private readonly ConcurrentDictionary<int, NotDeserializedRecord> unsupportedSchemaIds;
        private readonly ISchemaRegistryClient schemaRegistryClient;
        private readonly AvroDeserializerConfig avroDeserializerConfig;

        public MultiSchemaAvroDeserializer(IReadOnlyCollection<Type> types, ISchemaRegistryClient schemaRegistryClient, AvroDeserializerConfig avroDeserializerConfig = null)
        {
            deserializersBySchemaId = new ConcurrentDictionary<int, Func<ReadOnlyMemory<byte>, bool, SerializationContext, Task<ISpecificRecord>>>();
            unsupportedSchemaIds = new ConcurrentDictionary<int, NotDeserializedRecord>();
            this.schemaRegistryClient = schemaRegistryClient;
            this.avroDeserializerConfig = avroDeserializerConfig ?? new AvroDeserializerConfig();
            deserializersBySchema = GetSpecificSerializers(types, schemaRegistryClient);
        }

        private Dictionary<string, Func<ReadOnlyMemory<byte>, bool, SerializationContext, Task<ISpecificRecord>>> GetSpecificSerializers(IReadOnlyCollection<Type> specificRecordTypes, ISchemaRegistryClient schemaRegistry)
        {
            return specificRecordTypes?.Any() == true
                ? specificRecordTypes.ToDictionary(x => GetReaderSchema(x).Fullname, x => CreateSpecificSerializer(x, schemaRegistry))
                : new Dictionary<string, Func<ReadOnlyMemory<byte>, bool, SerializationContext, Task<ISpecificRecord>>>();
        }

        private static global::Avro.Schema GetReaderSchema(IReflect type)
        {
            return (global::Avro.Schema)type.GetField("_SCHEMA", BindingFlags.Public | BindingFlags.Static).GetValue(null);
        }

        private Func<ReadOnlyMemory<byte>, bool, SerializationContext, Task<ISpecificRecord>> CreateSpecificSerializer(Type specificType, ISchemaRegistryClient schemaRegistryClient)
        {
            var constructedAvroDeserializer = typeof(AvroDeserializer<>).MakeGenericType(specificType);
            var avroDeserializer = Activator.CreateInstance(constructedAvroDeserializer, schemaRegistryClient, avroDeserializerConfig);
            var openGenericMethod = typeof(MultiSchemaAvroDeserializer).GetMethod(nameof(DeserializeAsync), BindingFlags.Static | BindingFlags.NonPublic);
            var constructedGenericMethod = openGenericMethod.MakeGenericMethod(specificType);

            var parameters = constructedGenericMethod.GetParameters().Select(x =>
            {
                if (x.ParameterType.IsGenericType && x.ParameterType.GetGenericTypeDefinition().IsAssignableFrom(typeof(IAsyncDeserializer<>)))
                {
                    return Expression.Constant(avroDeserializer);
                }

                return (Expression)Expression.Parameter(x.ParameterType, x.Name);

            }).ToArray();

            var callExpression = Expression.Call(null, constructedGenericMethod, parameters);

            var lambda = Expression.Lambda<Func<ReadOnlyMemory<byte>, bool, SerializationContext, Task<ISpecificRecord>>>
                (
                    callExpression,
                    parameters.OfType<ParameterExpression>()
                );

            return lambda.Compile();
        }

        private static async Task<ISpecificRecord> DeserializeAsync<TSpecific>(ReadOnlyMemory<byte> message, bool isNull, SerializationContext context, IAsyncDeserializer<TSpecific> deserializer) where TSpecific : ISpecificRecord
        {
            return await deserializer.DeserializeAsync(message, isNull, context);
        }

        private async Task<Func<ReadOnlyMemory<byte>, bool, SerializationContext, Task<ISpecificRecord>>> GetSpecificDeserializer(int schemaId)
        {
            if (unsupportedSchemaIds.ContainsKey(schemaId))
            {
                return null;
            }

            if (deserializersBySchemaId.TryGetValue(schemaId, out var deserializer))
            {
                return deserializer;
            }

            var confluentSchema = await schemaRegistryClient.GetSchemaAsync(schemaId);
            var avroSchema = global::Avro.Schema.Parse(confluentSchema.SchemaString);

            if(deserializersBySchema.TryGetValue(avroSchema.Fullname, out deserializer))
            {
                deserializersBySchemaId.TryAdd(schemaId, deserializer);
            }
            else
            {
                unsupportedSchemaIds.TryAdd(schemaId, new NotDeserializedRecord(avroSchema, schemaId));
            }

            return deserializer;
        }

        public async Task<ISpecificRecord> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            if (data.Length < 5)
                return null;

            var schemaId = GetWriterSchemaId(data);

            var deserializer = await GetSpecificDeserializer(schemaId);

            if (deserializer == null)
            {
                if(unsupportedSchemaIds.TryGetValue(schemaId, out var unsupportedSchema))
                {
                    return new NotDeserializedRecord(unsupportedSchema.Schema, unsupportedSchema.SchemaId)
                    {
                        Data = data
                    };
                }

                return null;
            };

            return await deserializer(data, isNull, context);
        }

        private static int GetWriterSchemaId(ReadOnlyMemory<byte> data)
        {
            var firstFiveBytes = data.Span.Slice(0, 5);

            if (firstFiveBytes[0] != Constants.MagicByte)
            {
                throw new InvalidDataException($"Expecting data with Confluent Schema Registry framing. Magic byte was {firstFiveBytes[0]}, expecting 0");
            }

            var writerSchemaId = IPAddress.NetworkToHostOrder(MemoryMarshal.Read<int>(firstFiveBytes.Slice(1)));

            return writerSchemaId;
        }
    }
}
