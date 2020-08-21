using System;
using System.Reflection;
using Avro.Specific;
using Confluent.Kafka;

namespace Confluent.SchemaRegistry.Serdes
{
    internal static class TypeExtensions
    {
        public static global::Avro.Schema GetSchema<T>()
        {
            global::Avro.Schema resultSchema;

            if (typeof(ISpecificRecord).IsAssignableFrom(typeof(T)))
            {
                var defaultConstructor = typeof(T).GetConstructor(Type.EmptyTypes);
                if (defaultConstructor is null)
                {
                    throw new ArgumentException($"ISpecificRecord implementation '{typeof(T).FullName}' must have public default constructor.");
                }

                var record = (ISpecificRecord)Activator.CreateInstance<T>();
                resultSchema = record.Schema;
            }
            else if (typeof(T).Equals(typeof(int)))
            {
                resultSchema = Avro.Schema.Parse("int");
            }
            else if (typeof(T).Equals(typeof(bool)))
            {
                resultSchema = Avro.Schema.Parse("boolean");
            }
            else if (typeof(T).Equals(typeof(double)))
            {
                resultSchema = Avro.Schema.Parse("double");
            }
            else if (typeof(T).Equals(typeof(string)))
            {
                resultSchema = Avro.Schema.Parse("string");
            }
            else if (typeof(T).Equals(typeof(float)))
            {
                resultSchema = Avro.Schema.Parse("float");
            }
            else if (typeof(T).Equals(typeof(long)))
            {
                resultSchema = Avro.Schema.Parse("long");
            }
            else if (typeof(T).Equals(typeof(byte[])))
            {
                resultSchema = Avro.Schema.Parse("bytes");
            }
            else if (typeof(T).Equals(typeof(Null)))
            {
                resultSchema = Avro.Schema.Parse("null");
            }
            else
            {
                throw new InvalidOperationException(
                    $"{nameof(AvroDeserializer<T>)} " +
                    "only accepts type parameters of int, bool, double, string, float, " +
                    "long, byte[], instances of ISpecificRecord and subclasses of SpecificFixed."
                );
            }

            return resultSchema;
        }
    }
}