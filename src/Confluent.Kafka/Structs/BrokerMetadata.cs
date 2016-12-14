
namespace Confluent.Kafka
{
    /// <summary>
    ///     Metadata pertaining to a single Kafka broker.
    /// </summary>
    public struct BrokerMetadata
    {
        public int BrokerId { get; set; }
        public string Host { get; set; }
        public int Port { get; set; }

        public override string ToString()
        {
            return $"{{ \"BrokerId\": {BrokerId}, \"Host\": \"{Host}\", \"Port\": {Port} }}";
        }
    }
}
