namespace Confluent.Kafka
{
    public struct WatermarkOffsets
    {
        public WatermarkOffsets(Offset low, Offset high)
        {
            Low = low;
            High = high;
        }

        public Offset Low { get; }
        public Offset High { get; }
    }
}
