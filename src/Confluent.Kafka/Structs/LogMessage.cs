namespace Confluent.Kafka
{
    public struct LogMessage
    {
        public LogMessage(string name, int level, string facility, string message)
        {
            Name = name;
            Level = level;
            Facility = facility;
            Message = message;
        }

        public string Name { get; }
        public int Level { get; }
        public string Facility { get; }
        public string Message { get; }
    }
}
