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

        public string Topic { get; }
        public int Partition { get; }
        public Offset Offset { get; }

        public TopicPartition TopicPartition
        {
            get { return new TopicPartition(Topic, Partition); }
        }

        public override bool Equals(object obj)
        {
            if (!(obj is TopicPartitionOffset))
            {
                return false;
            }

            var tp = (TopicPartitionOffset)obj;
            return tp.Partition == Partition && tp.Topic == Topic && tp.Offset == Offset;
        }

        // x by prime number is quick and gives decent distribution.
        public override int GetHashCode()
            => (Partition.GetHashCode()*251 + Topic.GetHashCode())*251 + Offset.GetHashCode();

        public static bool operator ==(TopicPartitionOffset a, TopicPartitionOffset b)
            => a.Equals(b);

        public static bool operator !=(TopicPartitionOffset a, TopicPartitionOffset b)
            => !(a == b);

        public override string ToString()
            => $"{Topic} {Partition} {Offset}";
    }
}
