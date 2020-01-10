using Confluent.Kafka;
using System;
using System.Threading;


namespace Confluent.Kafka.Transactions
{
    public class TestProducer
    {
        static TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);
        Random random;

        SimulationConfig conf;
        int id;
        string bootstrapServers;

        public TestProducer(string bootstrapServers, int id, SimulationConfig simulationConfig)
        {
            this.id = id;
            this.bootstrapServers = bootstrapServers;

            // ensure different string of pseudo-random behavior for each test producer.
            random = new Random(simulationConfig.RandomSeedBase + id);

            conf = simulationConfig;
        }

        public void Run()
        {
            var pConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                TransactionalId = $"txn_test_{this.id}"
            };

            int lastMessageValue = 0;

            var producer = new ProducerBuilder<int, int>(pConfig).Build();
            producer.InitTransactions(DefaultTimeout);
            var currentState = ProducerState.InitState;

            for (int i=0; i<conf.MessageCount;)
            {
                Console.Write($"+{i}");
                Console.Out.Flush();

                // finalize previous state.
                switch (currentState)
                {
                    case ProducerState.MakingMessagesToAbort:
                        producer.AbortTransaction(DefaultTimeout);
                        break;
                    case ProducerState.MakingMessagesToCommit:
                        producer.CommitTransaction(DefaultTimeout);
                        break;
                    default:
                        // no action required.
                        break;
                }

                // transition to next state.
                var rnd = random.NextDouble();
                if (rnd < conf.ProbabilityLevel_Abort) { currentState = ProducerState.MakingMessagesToAbort; }
                else { currentState = ProducerState.MakingMessagesToCommit; }

                producer.BeginTransaction();
                int runLength = random.Next(conf.MaxRunLength);
                for (int j=0; j<runLength && i < conf.MessageCount; ++j, ++i)
                {
                    int val = currentState == ProducerState.MakingMessagesToCommit ? lastMessageValue++ : -1;
                    Thread.Sleep((int)(1000 * conf.MaxPauseSeconds));
                    producer.Produce(conf.Topic, new Message<int, int> { Key = id, Value = val });
                }
            }

            if (currentState == ProducerState.MakingMessagesToCommit) { producer.CommitTransaction(DefaultTimeout); }
            if (currentState == ProducerState.MakingMessagesToAbort) { producer.AbortTransaction(DefaultTimeout); }

            Console.WriteLine($"done: {id}");
            producer.Flush();
            producer.Dispose();
        }
    }
}
