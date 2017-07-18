using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.Runtime.Serialization;

namespace  Confluent.Kafka.SchemaRegistry.Rest.Entities
{
    [DataContract]
    public class Config
    {
        [DataContract(Name = "compatibility")]
        [JsonConverter(typeof(StringEnumConverter))]
        public enum Compatbility
        {
            [EnumMember(Value = "NONE")]
            NONE,
            [EnumMember(Value = "FORWARD")]
            FORWARD,
            [EnumMember(Value = "BACKWARD")]
            BACKWARD,
            [EnumMember(Value = "FULL")]
            FULL
        }

        [DataMember(Name = "compatibility")]
        public Compatbility CompatibilityLevel { get; }
        
        public Config(Compatbility compatibilityLevel)
        {
            CompatibilityLevel = compatibilityLevel;
        }

        public override string ToString() => $"{{compatibility={CompatibilityLevel}}}";
        
        public override bool Equals(object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }

            return CompatibilityLevel == ((Config)obj).CompatibilityLevel;
        }
        
        public override int GetHashCode()
        {
            return 31 * CompatibilityLevel.GetHashCode();
        }
    }
}
