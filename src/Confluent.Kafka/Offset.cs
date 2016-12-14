namespace Confluent.Kafka
{
    public struct Offset
    {
        private const long RD_KAFKA_OFFSET_BEGINNING = -2;
        private const long RD_KAFKA_OFFSET_END = -1;
        private const long RD_KAFKA_OFFSET_STORED = -1000;
        private const long RD_KAFKA_OFFSET_INVALID = -1001;

        public static Offset Invalid { get { return new Offset(RD_KAFKA_OFFSET_INVALID); } }
        public static Offset Beginning { get { return new Offset(RD_KAFKA_OFFSET_BEGINNING); } }
        public static Offset End { get { return new Offset(RD_KAFKA_OFFSET_END); } }
        public static Offset Stored { get { return new Offset(RD_KAFKA_OFFSET_STORED); } }

        public Offset(long offset)
        {
            Value = offset;
        }

        public long Value { get; private set; }

        // implicit cast between long and Offset struct.
        public static implicit operator Offset(long v)
        {
            return new Offset(v);
        }

        // implicit cast between Offset struct and long.
        public static implicit operator long(Offset o)
        {
            return o.Value;
        }

        public bool IsSpecial
        {
            get
            {
                return
                    Value == RD_KAFKA_OFFSET_BEGINNING ||
                    Value == RD_KAFKA_OFFSET_END ||
                    Value == RD_KAFKA_OFFSET_INVALID ||
                    Value == RD_KAFKA_OFFSET_STORED;
            }
        }
    }
}
