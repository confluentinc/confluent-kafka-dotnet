using System;
using Avro;
using Avro.Specific;

namespace Confluent.SchemaRegistry.Serdes.Avro
{
    public class NotDeserializedRecord : ISpecificRecord
    {
        public ReadOnlyMemory<byte> Data { get; internal set; }
        public global::Avro.Schema Schema { get; internal set; }
        public int SchemaId { get; internal set; }

        public object Get(int fieldPos)
        {
            throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()");
        }

        public void Put(int fieldPos, object fieldValue)
        {
            throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
        }

        public override string ToString()
        {
            return $"{{ SchemaName: {Schema.Name}, SchemaId: {SchemaId} }}";
        }
    }
}
