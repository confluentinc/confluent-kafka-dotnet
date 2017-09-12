using Newtonsoft.Json;
using System.Runtime.Serialization;

namespace  Confluent.Kafka.SchemaRegistry.Rest.Entities
{
    [DataContract]
    public class SchemaId
    {
        [DataMember(Name = "id")]
        public int Id { get; set; }

        /// <summary>
        /// Empty constructor for serialization
        /// </summary>
        private SchemaId() { }

        public SchemaId(int schemaId)
        {
            Id = schemaId;
        }
    }
}
