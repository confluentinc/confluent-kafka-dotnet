

namespace Confluent.Kafka.Transactions
{
    enum ProducerState
    {
        MakingMessagesToAbort,
        MakingMessagesToCommit,
        InitState
    }
}
