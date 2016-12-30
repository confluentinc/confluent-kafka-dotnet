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
        public string Topic { get; set; }
        public List<PartitionMetadata> Partitions { get; set; }
        public ErrorCode Error { get; set; }

        public override string ToString()
        {
            var result = new StringBuilder();
            result.Append($"{{ \"Topic\": \"{Topic}\", \"Partitions\": [");
            result.Append(string.Join(",", Partitions.Select(p => $" {p.ToString()}")));
            result.Append($" ], \"Error\": \"{Error.ToString()}\" }}");
            return result.ToString();
        }
    }
}
