namespace Confluent.Kafka
{
    // https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/consumer/OffsetAndTimestamp.java
    public struct OffsetAndTimestamp
    {
        public OffsetAndTimestamp(Offset offset, Timestamp timestamp)
        {
            Offset = offset;
            Timestamp = timestamp;
        }

        public Offset Offset { get; }

        public Timestamp Timestamp { get; }

        public override bool Equals(object obj)
        {
            if (!(obj is OffsetAndTimestamp))
            {
                return false;
            }

            var other = (OffsetAndTimestamp) obj;
            return Offset == other.Offset && Timestamp == other.Timestamp;
        }

        public override int GetHashCode()
            // x by prime number is quick and gives decent distribution.
            => Offset.GetHashCode() * 251 + Timestamp.GetHashCode();

        public override string ToString()
            => $"(timestamp={Timestamp}, offset={Offset})";
    }
}