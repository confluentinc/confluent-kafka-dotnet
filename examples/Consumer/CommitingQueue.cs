using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Confluent.Kafka.Examples.ConsumerExample;

public class CommitingQueue
{
    private AutoResetEvent parallelLock = new(true);
    private readonly IConsumer<Ignore, string> consumer;
    private Dictionary<int, SortedList<long, (ConsumeResult<Ignore, string> Message, bool Committed)>> messages;

    public CommitingQueue(IConsumer<Ignore, string> consumer)
    {
        this.consumer = consumer;
        messages = new Dictionary<int, SortedList<long, (ConsumeResult<Ignore, string> Message, bool Committed)>>();
    }

    public void Enqueue(ConsumeResult<Ignore, string> consumeResult)
    {
        messages.TryAdd(consumeResult.Partition.Value,
            new SortedList<long, (ConsumeResult<Ignore, string> Message, bool Committed)>());
        messages[consumeResult.Partition.Value].Add(consumeResult.Offset.Value, (consumeResult, false));
    }

    public void StoreOffset(ConsumeResult<Ignore, string> consumeResult)
    {
        parallelLock.WaitOne();
        var partionMessages = messages[consumeResult.Partition.Value];
        var valueTuple = partionMessages[consumeResult.Offset.Value];
        partionMessages[consumeResult.Offset.Value] = (valueTuple.Message, true);

        while (partionMessages.FirstOrDefault().Value != default && partionMessages.FirstOrDefault().Value.Committed)
        {
            var first = partionMessages.First();
            consumer.StoreOffset(first.Value.Message);
            partionMessages.Remove(first.Key);
        }

        parallelLock.Set();
    }
}