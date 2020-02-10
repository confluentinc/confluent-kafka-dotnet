
namespace Confluent.Kafka.Transactions
{
    public class SimulationConfig
    {
        public SimulationConfig(
            string topic,
            int messageCount,
            int randomSeedBase,
            int maxRunLength,
            double maxPause,
            double probability_abort)
        {
            Topic = topic;
            MessageCount = messageCount;
            RandomSeedBase = randomSeedBase;
            MaxRunLength = maxRunLength;
            MaxPauseSeconds = maxPause;
            ProbabilityLevel_Abort = probability_abort;
        }

        public string Topic { get; }
        public int MessageCount { get; }
        public int RandomSeedBase { get; }
        public int MaxRunLength { get; }
        public double MaxPauseSeconds { get; }
        public double ProbabilityLevel_Abort { get; }
        public double ProbabilityLevel_Commit { get => 1.0 - ProbabilityLevel_Abort; }
    }
}
