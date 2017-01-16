namespace Confluent.Kafka
{
    /// <summary>
    ///     Encapsulates a Topic / Partition / Offset / Error tuple.
    /// </summary>
    public struct TopicPartitionOffsetError
    {
        public TopicPartitionOffsetError(TopicPartition tp, Offset offset, Error error)
            : this(tp.Topic, tp.Partition, offset, error) {}

        public TopicPartitionOffsetError(TopicPartitionOffset tpo, Error error)
            : this(tpo.Topic, tpo.Partition, tpo.Offset, error) {}

        public TopicPartitionOffsetError(string topic, int partition, Offset offset, Error error)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
            Error = error;
        }

        public string Topic { get; }
        public int Partition { get; }
        public Offset Offset { get; }
        public Error Error { get; }

        public TopicPartition TopicPartition
            => new TopicPartition(Topic, Partition);

        public TopicPartitionOffset TopicPartitionOffset
            => new TopicPartitionOffset(Topic, Partition, Offset);

        public override bool Equals(object obj)
        {
            if (!(obj is TopicPartitionOffsetError))
            {
                return false;
            }

            var tp = (TopicPartitionOffsetError)obj;
            return tp.Partition == Partition && tp.Topic == Topic && tp.Offset == Offset && tp.Error == Error;
        }

        // x by prime number is quick and gives decent distribution.
        public override int GetHashCode()
            => ((Partition.GetHashCode()*251 + Topic.GetHashCode())*251 + Offset.GetHashCode())*251 + Error.GetHashCode();

        public static bool operator ==(TopicPartitionOffsetError a, TopicPartitionOffsetError b)
            => a.Equals(b);

        public static bool operator !=(TopicPartitionOffsetError a, TopicPartitionOffsetError b)
            => !(a == b);

        public override string ToString()
            => $"{Topic} {Partition} {Offset} {Error}";
    }
}
