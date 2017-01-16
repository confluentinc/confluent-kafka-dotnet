namespace Confluent.Kafka
{
    public struct TopicPartition
    {
        public TopicPartition(string topic, int partition)
        {
            Topic = topic;
            Partition = partition;
        }

        public string Topic { get; }
        public int Partition { get; }

        public override bool Equals(object obj)
        {
            if (!(obj is TopicPartition))
            {
                return false;
            }

            var tp = (TopicPartition)obj;
            return tp.Partition == Partition && tp.Topic == Topic;
        }

        // x by prime number is quick and gives decent distribution.
        public override int GetHashCode()
            => Partition.GetHashCode()*251 + Topic.GetHashCode();

        public static bool operator ==(TopicPartition a, TopicPartition b)
            => a.Equals(b);

        public static bool operator !=(TopicPartition a, TopicPartition b)
            => !(a == b);

        public override string ToString()
            => $"{Topic} {Partition}";
    }
}
