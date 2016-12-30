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
        /// <summary>
        ///     Information about each constituent broker of the cluster.
        /// </summary>
        public List<BrokerMetadata> Brokers { get; set; }

        /// <summary>
        ///     Information about every topic managed by in the cluster.
        /// </summary>
        public List<TopicMetadata> Topics { get; set; }

        /// <summary>
        /// </summary>
        public int OriginatingBrokerId { get; set; }

        /// <summary>
        /// </summary>
        public string OriginatingBrokerName { get; set; }

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