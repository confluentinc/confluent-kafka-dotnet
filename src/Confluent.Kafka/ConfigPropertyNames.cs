namespace Confluent.Kafka
{
    /// <summary>
    ///     Names of all configuration properties specific to the
    ///     .NET Client.
    /// </summary>
    public static class ConfigPropertyNames
    {
        // ---- Producer

        /// <summary>
        ///     Specifies whether or not the producer should start a background poll 
        ///     thread to receive delivery reports and event notifications. Generally,
        ///     this should be set to true. If set to false, you will need to call 
        ///     the Poll function manually.
        /// 
        ///     default: true
        /// </summary>
        public const string ProducerEnableBackgroundPoll = "dotnet.producer.enable.background.poll";

        /// <summary>
        ///     Specifies whether to enable notification of delivery reports. Typically
        ///     you should set this parameter to true. Set it to false for "fire and
        ///     forget" semantics and a small boost in performance.
        /// 
        ///     default: true
        /// </summary>
        public const string ProducerEnableDeliveryReports = "dotnet.producer.enable.delivery.reports";

        /// <summary>
        ///     A comma separated list of fields that may be optionally set in delivery
        ///     reports. Disabling delivery report fields that you do not require will
        ///     improve maximum throughput and reduce memory usage. Allowed values:
        ///     key, value, timestamp, headers, all, none.
        /// 
        ///     default: all
        /// </summary>
        public const string ProducerDeliveryReportFields = "dotnet.producer.delivery.report.fields";

        // ---- Consumer

        /// <summary>
        ///     A comma separated list of fields that may be optionally set
        ///     in <see cref="Confluent.Kafka.ConsumeResult{TKey, TValue}" />
        ///     objects returned by the
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Consume(System.TimeSpan)" />
        ///     method. Disabling fields that you do not require will improve 
        ///     throughput and reduce memory consumption. Allowed values:
        ///     headers, timestamp, topic, all, none
        /// 
        ///     default: all
        /// </summary>
        public const string ConsumerConsumeResultFields = "dotnet.consumer.consume.result.fields";


        // ---- Deserializers

        /// <summary>
        ///     Specifies the encoding when deserializing string keys.
        /// </summary>
        public const string StringDeserializerEncodingKey = "dotnet.string.deserializer.encoding.key";

        /// <summary>
        ///     Specifies the encoding when deserializing string values.
        /// </summary>
        public const string StringDeserializerEncodingValue = "dotnet.string.deserializer.encoding.value";


        // ---- Serializers

        /// <summary>
        ///     Specifies the encoding when serializing string keys.
        /// </summary>
        public const string StringSerializerEncodingKey = "dotnet.string.serializer.encoding.key";

        /// <summary>
        ///     Specifies the encoding when serializing string values.
        /// </summary>
        public const string StringSerializerEncodingValue = "dotnet.string.serializer.encoding.value";


        /// <summary>
        ///     Specifies the initial size (in bytes) of the buffer used for Avro message
        ///     serialization. Use a value high enough to avoid resizing the buffer, but
        ///     small enough to avoid excessive memory use. Inspect the size of the byte
        ///     array returned by the Serialize method to estimate an appropriate value.
        ///     Note: each call to serialize creates a new buffer.
        /// 
        ///     default: 1024
        /// </summary>
        public const string AvroSerializerBufferBytes = "avro.serializer.buffer.bytes";

        /// <summary>
        ///     Specifies whether or not the Avro serializer should attempt to auto-register
        ///     unrecognized schemas with Confluent Schema Registry.
        ///
        ///     default: true
        /// </summary>
        public const string AvroSerializerAutoRegisterSchemas = "avro.serializer.auto.register.schemas";


        // ---- Schema Registry Client

        /// <summary>
        ///     A comma-separated list of URLs for schema registry instances that are
        ///     used to register or lookup schemas.
        /// </summary>
        public const string SchemaRegistryUrl = "schema.registry.url";

        /// <summary>
        ///     Specifies the timeout for requests to Confluent Schema Registry.
        /// 
        ///     default: 30000
        /// </summary>
        public const string SchemaRegistryRequestTimeoutMs = "schema.registry.request.timeout.ms";

        /// <summary>
        ///     Specifies the maximum number of schemas CachedSchemaRegistryClient
        ///     should cache locally.
        /// 
        ///     default: 1000
        /// </summary>
        public const string SchemaRegistryMaxCachedSchemas = "schema.registry.max.cached.schemas";
    }
}