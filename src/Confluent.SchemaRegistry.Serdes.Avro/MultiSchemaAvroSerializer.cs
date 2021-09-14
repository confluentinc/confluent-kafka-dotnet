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
        private readonly ConcurrentDictionary<Type, Func<ISpecificRecord, SerializationContext, Task<byte[]>>> serializersByType;

        public MultiSchemaAvroSerializer(ISchemaRegistryClient schemaRegistryClient, AvroSerializerConfig avroSerializerConfig = null)
        {
            this.schemaRegistryClient = schemaRegistryClient ?? throw new ArgumentNullException(nameof(schemaRegistryClient));
            this.avroSerializerConfig = avroSerializerConfig;
            serializersByType = new ConcurrentDictionary<Type, Func<ISpecificRecord, SerializationContext, Task<byte[]>>>();
        }

        public Task<byte[]> SerializeAsync(ISpecificRecord data, SerializationContext context)
        {
            return data == null ? Task.FromResult(Array.Empty<byte>()) : GetOrCreateSerializer(data.GetType())(data, context);
        }

        private Func<ISpecificRecord, SerializationContext, Task<byte[]>> GetOrCreateSerializer(Type specificRecordType)
        {
            return serializersByType.GetOrAdd(specificRecordType, CreateSerializer);
        }

        private Func<ISpecificRecord, SerializationContext, Task<byte[]>> CreateSerializer(Type specificType)
        {
            var constructedSerializerType = typeof(AvroSerializer<>).MakeGenericType(specificType);
            var serializerInstance = Activator.CreateInstance(constructedSerializerType, schemaRegistryClient, avroSerializerConfig);
            var openGenericSerializeMethod = typeof(MultiSchemaAvroSerializer).GetMethod(nameof(SerializeAsync), BindingFlags.Static | BindingFlags.NonPublic) ?? throw new MissingMethodException(nameof(MultiSchemaAvroDeserializer), nameof(SerializeAsync));
            var constructedSerializeMethod = openGenericSerializeMethod.MakeGenericMethod(specificType);

            var parameters = constructedSerializeMethod.GetParameters().Select(x =>
            {
                if (x.ParameterType.IsGenericType && x.ParameterType.GetGenericTypeDefinition().IsAssignableFrom(typeof(IAsyncSerializer<>)))
                {
                    return Expression.Constant(serializerInstance);
                }

                return (Expression)Expression.Parameter(x.ParameterType, x.Name);

            }).ToArray();

            var callExpression = Expression.Call(null, constructedSerializeMethod, parameters);
            var lambda = Expression.Lambda<Func<ISpecificRecord, SerializationContext, Task<byte[]>>>(callExpression, parameters.OfType<ParameterExpression>());

            return lambda.Compile();
        }

        private static Task<byte[]> SerializeAsync<T>(IAsyncSerializer<T> serializer, ISpecificRecord specificRecord, SerializationContext context)
        {
            return serializer.SerializeAsync((T)specificRecord, context);
        }
    }
}
