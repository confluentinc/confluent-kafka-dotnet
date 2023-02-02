namespace Confluent.Kafka.Diagnostics
{
    /// <summary>
    /// The conventions in OTel are still work-in-progress (Status: Experimental).
    /// </summary>
    internal static class Conventions
    {
        internal static class OperationName
        {
            /// <summary>
            /// A message is sent to a destination by a message producer/client.
            /// </summary>
            public const string Send = "send";

            /// <summary>
            /// A message is received from a destination by a message consumer/server.
            /// </summary>
            public const string Receive = "receive";

            /// <summary>
            /// A message that was previously received from a destination is processed by a message consumer/server.
            /// </summary>
            public const string Process = "process";
        }

        /// <summary>
        /// Provides the OpenTelemetry messaging attributes.
        /// The complete list of messaging attributes specification is available here: https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md#messaging-attributes
        /// </summary>
        internal static class MessagingTag
        {
            /// <summary>
            /// A string identifying the messaging system.
            /// For Kafka, attribute value must be "kafka".
            /// </summary>
            public const string System = "messaging.system";

            /// <summary>
            /// The message destination name. This might be equal to the span name but is required nevertheless.
            /// For Kafka, attribute value must be a Kafka topic.
            /// </summary>
            public const string Destination = "messaging.destination";

            /// <summary>
            /// The kind of message destination.
            /// For Kafka, attribute value must be "topic".
            /// </summary>
            public const string DestinationKind = "messaging.destination_kind";

            /// <summary>
            /// The (uncompressed) size of the message payload in bytes.
            /// Also use this attribute if it is unknown whether the compressed or uncompressed payload size is reported.
            /// </summary>
            public const string MessagePayloadSizeBytes = "messaging.message_payload_size_bytes";

            /// <summary>
            /// A string identifying the kind of message consumption. If the operation is "send", this attribute MUST NOT be set.
            /// </summary>
            public const string Operation = "messaging.operation";

            /// <summary>
            /// The identifier for the consumer receiving a message.
            /// For Kafka, set it to {messaging.kafka.consumer_group} - {messaging.kafka.client_id}, if both are present, or only messaging.kafka.consumer_group.
            /// </summary>
            public const string ConsumerId = "messaging.consumer_id";
        }

        /// <summary>
        /// Provides the OpenTelemetry additional attributes for Kafka.
        /// The complete list of Kafka attributes specification is available here: https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md#apache-kafka
        /// </summary>
        internal static class KafkaTag
        {
            /// <summary>
            /// Message keys in Kafka are used for grouping alike messages to ensure they're processed on the same partition.
            /// </summary>
            public const string MessageKey = "messaging.kafka.message_key";

            /// <summary>
            /// Name of the Kafka Consumer Group that is handling the message. Only applies to consumers, not producers.
            /// </summary>
            public const string ConsumerGroup = "messaging.kafka.consumer_group";

            /// <summary>
            /// Client Id for the Consumer or Producer that is handling the message.
            /// </summary>
            public const string ClientId = "messaging.kafka.client_id";

            /// <summary>
            /// Partition the message is sent to.
            /// </summary>
            public const string Partition = "messaging.kafka.partition";

            /// <summary>
            /// A boolean that is true if the message is a tombstone.
            /// </summary>
            public const string Tombstone = "messaging.kafka.tombstone";
        }
    }
}
