using System.Runtime.Serialization;

namespace  Confluent.Kafka.SchemaRegistry.Rest.Entities.Requests
{
    [DataContract]
    public class CompatibilityCheckResponse
    {
        [DataMember(Name ="is_compatible")]
        public bool IsCompatible { get; set; }

        public CompatibilityCheckResponse(bool isCompatible)
        {
            IsCompatible = isCompatible;
        }
    }
}
