
namespace Confluent.Kafka
{
    /// <summary>
    ///     Metadata pertaining to a single Kafka broker.
    /// </summary>
    public struct BrokerMetadata
    {
        public BrokerMetadata(int brokerId, string host, int port)
        {
            BrokerId = brokerId;
            Host = host;
            Port = port;
        }

        public int BrokerId { get; }
        public string Host { get; }
        public int Port { get; }

        public override string ToString()
            => $"{{ \"BrokerId\": {BrokerId}, \"Host\": \"{Host}\", \"Port\": {Port} }}";
    }
}
