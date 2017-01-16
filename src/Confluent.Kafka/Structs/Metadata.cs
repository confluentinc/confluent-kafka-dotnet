using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace Confluent.Kafka
{
    /// <summary>
    ///     Metadata pertaining to a single Kafka cluster
    /// </summary>
    public struct Metadata
    {
        public Metadata(List<BrokerMetadata> brokers, List<TopicMetadata> topics, int originatingBrokerId, string originatingBrokerName)
        {
            Brokers = brokers;
            Topics = topics;
            OriginatingBrokerId = originatingBrokerId;
            OriginatingBrokerName = originatingBrokerName;
        }

        /// <summary>
        ///     Information about each constituent broker of the cluster.
        /// </summary>
        public List<BrokerMetadata> Brokers { get; }

        /// <summary>
        ///     Information about every topic managed by in the cluster.
        /// </summary>
        public List<TopicMetadata> Topics { get; }

        /// <summary>
        /// </summary>
        public int OriginatingBrokerId { get; }

        /// <summary>
        /// </summary>
        public string OriginatingBrokerName { get; }

        public override string ToString()
        {
            var result = new StringBuilder();
            result.Append($"{{ \"OriginatingBrokerId\": {OriginatingBrokerId}, \"OriginatingBrokerName\": \"{OriginatingBrokerName}\", \"Brokers\": [");
            result.Append(string.Join(",", Brokers.Select(b => $" {b.ToString()}")));
            result.Append($" ], \"Topics\": [");
            result.Append(string.Join(",", Topics.Select(t => $" {t.ToString()}")));
            result.Append("] }");
            return result.ToString();
        }
    }
}
