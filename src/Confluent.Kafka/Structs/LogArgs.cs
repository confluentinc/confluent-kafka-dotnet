namespace Confluent.Kafka
{
    public struct LogArgs
    {
        public string Name { get; set; }
        public int Level { get; set; }
        public string Facility { get; set; }
        public string Message { get; set; }
    }
}