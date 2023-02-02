using System.Diagnostics;
using static Confluent.Kafka.Diagnostics.Conventions;

namespace Confluent.Kafka.Diagnostics
{
    internal static class Diagnostic
    {
        private const string ActivitySourceName = "Confluent.Kafka";
        private const string System = "kafka";
        private const string DestinationKind = "topic";

        public static ActivitySource ActivitySource { get; } = new ActivitySource(ActivitySourceName);

        /// <summary>
        /// The span name SHOULD be set to the message destination name and the operation
        /// https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md#span-name
        /// </summary>
        /// <param name="topic">A Kafka topic name.</param>
        /// <param name="operationName">Operation name.</param>
        /// <returns></returns>
        private static string GetSpanName(string topic, string operationName)
        {
            return $"{topic} {operationName}";
        }

        internal static class Producer
        {
            internal static Activity StartActivity<TKey, TValue>(TopicPartition topicPartition, Message<TKey, TValue> message)
            {
                var spanName = GetSpanName(topicPartition.Topic, OperationName.Send);
                var initialTags = GetInitialTags(topicPartition, message);

                ActivityContext parentContext = Activity.Current?.Context ?? default;

                var activity = ActivitySource.StartActivity(
                    spanName,
                    ActivityKind.Producer,
                    parentContext,
                    initialTags);

                if (activity?.IsAllDataRequested ?? false)
                {
                    // A message is an envelope with a potentially empty payload.
                    // This envelope may offer the possibility to convey additional metadata, often in key / value form.
                    // TODO: How to get the size of the message?
                    // Is message.ToString() (or message.Value.ToString()) going to be very expensive?
                    // Ideally getting the size would either be cheap or it would be an opt-in thing for users who need it enough to pay for it.
                    //var messagePayloadBytes = Encoding.UTF8.GetByteCount(message.ToString() ?? string.Empty); // TODO: we can return null
                    //activity.SetTag(MessagingTag.MessagePayloadSizeBytes, messagePayloadBytes);
                }

                return activity;
            }

            private static ActivityTagsCollection GetInitialTags<TKey, TValue>(
                TopicPartition topicPartition,
                Message<TKey, TValue> message)
            {
                var initialTags = new ActivityTagsCollection
                {
                    [MessagingTag.System] = System,
                    [MessagingTag.Destination] = topicPartition.Topic,
                    [MessagingTag.DestinationKind] = DestinationKind,

                    // Apache Kafka
                    // https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md#apache-kafka
                    //[KAFKA_MESSAGE_KEY] = ??, TODO: Type = string... convert or serialize?
                    //[KAFKA_CLIENT_ID] = ??, // TODO: where can I get the data?
                    [KafkaTag.Partition] = topicPartition.Partition.Value,
                    //[KAFKA_TOMBSTONE] = ??, // A boolean that is true if the message is a tombstone. TODO: What is it and where can I get the data?
                };
                return initialTags;
            }
        }

        internal static class Consumer
        {
            internal static Activity StartActivity<TKey, TValue>(string topic, int partition)
            {
                var spanName = GetSpanName(topic, OperationName.Process);
                var initialTags = GetInitialTags<TKey, TValue>(topic, partition, OperationName.Process);

                ActivityContext parentContext = default;
                var activity = ActivitySource.StartActivity(
                    spanName,
                    ActivityKind.Consumer,
                    parentContext,
                    initialTags);

                if (activity?.IsAllDataRequested ?? false)
                {
                    //activity?.SetTag(KAFKA_CONSUMER_GROUP, )

                    // A message is an envelope with a potentially empty payload.
                    // This envelope may offer the possibility to convey additional metadata, often in key / value form.
                    // TODO: How to get the size of the message?
                    // Is message.ToString() (or message.Value.ToString()) going to be very expensive?
                    // Ideally getting the size would either be cheap or it would be an opt-in thing for users who need it enough to pay for it.
                    //var messagePayloadBytes = Encoding.UTF8.GetByteCount(message.ToString() ?? string.Empty); // TODO: we can return null
                    //activity.SetTag(MessagingTag.MessagePayloadSizeBytes, messagePayloadBytes);
                }

                return activity;
            }
        }

        private static ActivityTagsCollection GetInitialTags<TKey, TValue>(
            string topic,
            int partition,
            string operation)
        {
            var initialTags = new ActivityTagsCollection
            {
                [MessagingTag.System] = System,
                [MessagingTag.Destination] = topic,
                [MessagingTag.DestinationKind] = DestinationKind,

                [MessagingTag.Operation] = operation,
                [MessagingTag.ConsumerId] = "", // TODO: add consumer_id

                // Apache Kafka
                // https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md#apache-kafka
                //[KAFKA_MESSAGE_KEY] = ??, TODO: Type = string... convert or serialize?
                //[KAFKA_CONSUMER_GROUP] = ??, TODO: where can I get the data? Only applies to consumers, not producers.
                //[KAFKA_CLIENT_ID] = ??, // TODO: where can I get the data?
                [KafkaTag.Partition] = partition,
                //[KAFKA_TOMBSTONE] = ??, // A boolean that is true if the message is a tombstone. TODO: What is it and where can I get the data?
            };
            return initialTags;
        }
    }
}
