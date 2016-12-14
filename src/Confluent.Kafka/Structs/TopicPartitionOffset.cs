namespace Confluent.Kafka
{
    /// <summary>
    ///     Encapsulates a Topic / Partition / Offset tuple.
    /// </summary>
    /// <remarks>
    ///     Partition must be defined.
    ///     Offset may be special value.
    /// </remarks>
    public struct TopicPartitionOffset
    {
        public TopicPartitionOffset(TopicPartition tp, Offset offset)
        {
            Topic = tp.Topic;
            Partition = tp.Partition;
            Offset = offset;
        }

        public TopicPartitionOffset(string topic, int partition, Offset offset)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
        }

        public string Topic { get; set; }
        public int Partition { get; set; }
        public Offset Offset { get; set; }

        public TopicPartition TopicPartition
        {
            get
            {
                return new TopicPartition
                {
                    Topic = Topic,
                    Partition = Partition
                };
            }
        }

        public override string ToString() => $"{Topic} {Partition} {Offset}";
    }
}
