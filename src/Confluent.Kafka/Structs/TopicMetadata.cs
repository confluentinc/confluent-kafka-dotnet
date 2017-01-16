using System.Collections.Generic;
using System.Linq;
using System.Text;


namespace Confluent.Kafka
{

    /// <summary>
    ///     Metadata pertaining to a single Kafka topic.
    /// </summary>
    public struct TopicMetadata
    {
        public TopicMetadata(string topic, List<PartitionMetadata> partitions, Error error)
        {
            Topic = topic;
            Error = error;
            Partitions = partitions;
        }

        public string Topic { get; }
        public List<PartitionMetadata> Partitions { get; }
        public Error Error { get; }

        public override string ToString()
        {
            var result = new StringBuilder();
            result.Append($"{{ \"Topic\": \"{Topic}\", \"Partitions\": [");
            result.Append(string.Join(",", Partitions.Select(p => $" {p.ToString()}")));
            result.Append($" ], \"Error\": \"{Error.Code.ToString()}\" }}");
            return result.ToString();
        }
    }
}
