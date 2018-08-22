using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka.Serialization;

namespace Confluent.Kafka
{
    public class ConsumerQueue<TKey, TValue> : Consumer<TKey, TValue>
    {
        private volatile Dictionary<TopicPartition, IntPtr> queues;

        public ConsumerQueue(
            IEnumerable<KeyValuePair<string, object>> config,
            IDeserializer<TKey> keyDeserializer,
            IDeserializer<TValue> valueDeserializer)
            : base(config, keyDeserializer, valueDeserializer)
        {
        }

        public override void Assign(IEnumerable<TopicPartitionOffset> partitions)
        {
            base.Assign(partitions);

            var queue = partitions.ToDictionary(
                p => p.TopicPartition,
                p => kafkaHandle.ForwardPartitionToQueue(p.TopicPartition));

            queues = queue;
        }

        public override void Unassign()
        {
            // Unforward queues per https://github.com/edenhill/librdkafka/issues/1335

            foreach (var rkqu in queues.Values)
            {
                kafkaHandle.RestoreQueue(rkqu);
                kafkaHandle.DestroyQueue(rkqu);
            }

            queues = null;

            base.Unassign();
        }

        public IReadOnlyCollection<ConsumeResult<TKey, TValue>> ConsumeBatch(TopicPartition partition, int batchSize, int millisecondsTimeout)
        {
            var rkqu = queues[partition];
            var messages = new IntPtr[batchSize];
            var numMessages = kafkaHandle.QueuePoll(rkqu, messages, millisecondsTimeout);
            var results = new ConsumeResult<TKey, TValue>[numMessages];
            for (int i = 0; i < numMessages; i++)
            {
                results[i] = GetMessage(messages[i]);
            }
            return results;
        }
    }
}
