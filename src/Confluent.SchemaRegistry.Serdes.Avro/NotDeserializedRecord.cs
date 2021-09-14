using System;
using Avro;
using Avro.Specific;

namespace Confluent.SchemaRegistry.Serdes.Avro
{
    public class NotDeserializedRecord : ISpecificRecord
    {
        public NotDeserializedRecord()
        {
        }

        public NotDeserializedRecord(global::Avro.Schema schema, int schemaId)
        {
            Schema = schema;
            SchemaId = schemaId;
        }

        public ReadOnlyMemory<byte> Data { get; internal set; }

        public object Get(int fieldPos)
        {
            switch (fieldPos)
            {
                case 0:
                    return Data;
                default:
                    throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()");
            }
        }

        public void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0:
                    Data = (ReadOnlyMemory<byte>)fieldValue;
                    break;
                default:
                    throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
            }
        }

        public global::Avro.Schema Schema { get; }
        public int SchemaId { get; }

        public override string ToString()
        {
            return $"{nameof(NotDeserializedRecord)}: {{ SchemaName: {Schema.Name}, SchemaId: {SchemaId} }}";
        }
    }
}
