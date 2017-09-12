using System.Runtime.Serialization;

namespace  Confluent.Kafka.SchemaRegistry.Rest.Entities
{
    /// <summary>
    /// Generic JSON error message.
    /// </summary>
    public class ErrorMessage
    {
        [DataMember(Name ="error_code")]
        public int ErrorCode { get; set; }

        [DataMember(Name = "message")]
        public string Message { get; set; }

        public ErrorMessage(int errorCode, string message)
        {
            ErrorCode = errorCode;
            Message = message;
        }
        public override string ToString()
        {
            return $"{{error_code={ErrorCode},message={Message}}}";
        }
    }
}
