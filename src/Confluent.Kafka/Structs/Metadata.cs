using System.Collections.Generic;
using System.Text;


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

            for (int i=0; i<Brokers.Count; ++i)
            {
                result.Append($" {Brokers[i].ToString()}");
                if (i != Brokers.Count-1)
                {
                    result.Append(",");
                }
            }

            result.Append($" ], \"Topics\": [");

            for (int i=0; i<Topics.Count; ++i)
            {
                result.Append($" {Topics[i].ToString()}");
                if (i != Topics.Count-1)
                {
                    result.Append(",");
                }
            }

            result.Append("] }");

            return result.ToString();
        }
    }
}