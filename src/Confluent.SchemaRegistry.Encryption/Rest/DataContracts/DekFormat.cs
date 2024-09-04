using System.Runtime.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Confluent.SchemaRegistry.Encryption
{
    /// <summary>
    ///     Dek format.
    /// </summary>
    [DataContract(Name = "dekFormat")]
    [JsonConverter(typeof(StringEnumConverter))]
    public enum DekFormat
    {
        AES256_SIV,
        AES128_GCM,
        AES256_GCM
    }
}