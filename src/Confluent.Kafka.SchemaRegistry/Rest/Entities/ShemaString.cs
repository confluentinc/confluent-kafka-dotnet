using Newtonsoft.Json;
using System.Runtime.Serialization;

namespace  Confluent.Kafka.SchemaRegistry.Rest.Entities
{
    [DataContract]
    public class SchemaString
    {
        [DataMember(Name ="schema")]
        public string Schema { get; set; }

        /// <summary>
        /// Empty constructor for serialization
        /// </summary>
        private SchemaString() { }

        public SchemaString(string schemaString)
        {
            Schema = schemaString;
        }
    }
}
