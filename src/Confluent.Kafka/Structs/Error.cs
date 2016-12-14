namespace Confluent.Kafka
{
    public struct Error
    {
        public Error(ErrorCode code, string message)
        {
            Message = message;
            Code = code;
        }

        public string Message { get; set; }
        public ErrorCode Code { get; set; }
    }
}