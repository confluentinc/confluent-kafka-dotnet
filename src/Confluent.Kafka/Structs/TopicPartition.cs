namespace Confluent.Kafka
{
    public struct TopicPartition
    {
        public TopicPartition(string topic, int partition)
        {
            Topic = topic;
            Partition = partition;
        }

        public string Topic { get; set; }
        public int Partition { get; set; }

        public override string ToString() => $"{Topic} {Partition}";
    }
}