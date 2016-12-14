namespace Confluent.Kafka
{
    public struct ErrorArgs
    {
        public ErrorCode ErrorCode { get; set; }
        public string Reason { get; set; }
    }
}