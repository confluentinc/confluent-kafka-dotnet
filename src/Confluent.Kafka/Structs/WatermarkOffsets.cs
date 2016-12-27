namespace Confluent.Kafka
{
    public struct WatermarkOffsets
    {
        public Offset Low { get; set; }
        public Offset High { get; set; }
    }
}
