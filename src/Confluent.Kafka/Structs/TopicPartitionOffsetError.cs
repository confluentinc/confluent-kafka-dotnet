namespace Confluent.Kafka
{
    /// <summary>
    ///     Encapsulates a Topic / Partition / Offset / Error tuple.
    /// </summary>
    /// <remarks>
    ///     Partition must be defined.
    ///     Offset may be special value.
    /// </remarks>
    public struct TopicPartitionOffsetError
    {
        public TopicPartitionOffsetError(string topic, int partition, Offset offset, Error error)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
            Error = error;
        }

        public string Topic { get; set; }
        public int Partition { get; set; }
        public Offset Offset { get; set; }
        public Error Error { get; set; }

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

        public TopicPartitionOffset TopicPartitionOffset
        {
            get
            {
                return new TopicPartitionOffset
                {
                    Topic = Topic,
                    Partition = Partition,
                    Offset = Offset
                };
            }
        }

        public override string ToString() => $"{Topic} {Partition} {Offset} {Error}";
    }
}
