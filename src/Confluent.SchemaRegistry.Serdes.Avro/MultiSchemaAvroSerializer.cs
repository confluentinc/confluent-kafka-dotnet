using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Avro.Specific;
using Confluent.Kafka;

namespace Confluent.SchemaRegistry.Serdes.Avro
{
    public class MultiSchemaAvroSerializer : IAsyncSerializer<ISpecificRecord>
    {
        private readonly ISchemaRegistryClient schemaRegistryClient;
        private readonly AvroSerializerConfig avroSerializerConfig;
        private readonly ConcurrentDictionary<Type, Func<ISpecificRecord, SerializationContext, Task<byte[]>>> specificSerializers;

        public MultiSchemaAvroSerializer(ISchemaRegistryClient schemaRegistryClient, AvroSerializerConfig avroSerializerConfig = null)
        {
            this.schemaRegistryClient = schemaRegistryClient ?? throw new ArgumentNullException(nameof(schemaRegistryClient));
            this.avroSerializerConfig = avroSerializerConfig ?? new AvroSerializerConfig();
            specificSerializers = new ConcurrentDictionary<Type, Func<ISpecificRecord, SerializationContext, Task<byte[]>>>();
        }

        public Task<byte[]> SerializeAsync(ISpecificRecord data, SerializationContext context)
        {
            return data == null ? null : GetOrCreateSerializer(data.GetType())(data, context);
        }

        private Func<ISpecificRecord, SerializationContext, Task<byte[]>> GetOrCreateSerializer(Type specificRecordType)
        {
            return specificSerializers.GetOrAdd(specificRecordType, CreateSerializer);
        }

        private Func<ISpecificRecord, SerializationContext, Task<byte[]>> CreateSerializer(Type specificType)
        {
            var constructedAvroSerializer = typeof(AvroSerializer<>).MakeGenericType(specificType);
            var avroSerializer = Activator.CreateInstance(constructedAvroSerializer, schemaRegistryClient, avroSerializerConfig);

            var openGenericMethod = typeof(MultiSchemaAvroSerializer).GetMethod(nameof(SerializeAsync), BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic) ?? throw new MissingMethodException(nameof(MultiSchemaAvroDeserializer), nameof(SerializeAsync));
            var constructedGenericMethod = openGenericMethod.MakeGenericMethod(specificType);
            var parameters = constructedGenericMethod.GetParameters().Select(x =>
            {
                if (x.ParameterType.IsGenericType && x.ParameterType.GetGenericTypeDefinition().IsAssignableFrom(typeof(IAsyncSerializer<>)))
                {
                    return Expression.Constant(avroSerializer);
                }

                return (Expression)Expression.Parameter(x.ParameterType, x.Name);

            }).ToArray();

            var callExpression = Expression.Call(null, constructedGenericMethod, parameters);
            var lambda = Expression.Lambda<Func<ISpecificRecord, SerializationContext, Task<byte[]>>>(callExpression, parameters.OfType<ParameterExpression>());

            return lambda.Compile();
        }

        private static async Task<byte[]> SerializeAsync<T>(IAsyncSerializer<T> serializer, ISpecificRecord specificRecord, SerializationContext context)
        {
            return await serializer.SerializeAsync((T)specificRecord, context);
        }
    }
}
