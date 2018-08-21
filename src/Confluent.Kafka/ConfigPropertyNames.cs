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
        public const string EnableBackgroundPollPropertyName = "dotnet.producer.enable.background.poll";

        /// <summary>
        ///     Specifies whether to enable notification of delivery reports. Typically
        ///     you should set this parameter to true. Set it to false for "fire and
        ///     forget" semantics and a small boost in performance.
        /// 
        ///     default: true
        /// </summary>
        public const string EnableDeliveryReportsPropertyName = "dotnet.producer.enable.delivery.reports";

        /// <summary>
        ///     A comma separated list of fields that may be optionally set in delivery
        ///     reports. Disabling delivery report fields that you do not require will
        ///     improve maximum throughput and reduce memory usage. Allowed values:
        ///     key, value, timestamp, headers, all, none.
        /// 
        ///     default: all
        /// </summary>
        public const string DeliveryReportEnabledFieldsPropertyName = "dotnet.producer.delivery.report.enabled.fields";

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
        public const string ConsumerEnabledFieldsPropertyName = "dotnet.consumer.enabled.fields";


        // ---- Client

        /// <summary>
        ///     Name of the configuration property that specifies a delegate for
        ///     handling log messages. If not specified, a default callback that
        ///     writes to stderr will be used.
        /// </summary>
        /// <remarks>
        ///     By default not many log messages are generated.
        ///
        ///     For more verbose logging, specify one or more debug contexts 
        ///     using the 'debug' configuration property. The 'log_level'
        ///     configuration property is also relevant, however logging is
        ///     verbose by default given a debug context has been specified,
        ///     so you typically shouldn't adjust this value.
        ///
        ///     Warning: Log handlers are called spontaneously from internal 
        ///     librdkafka threads and the application must not call any 
        ///     Confluent.Kafka APIs from within a log handler or perform any
        ///     prolonged operations.
        /// </remarks>
        public const string LogDelegateName = "log_cb";

        /// <summary>
        ///     Name of the configuration property that specifies a delegate for
        ///     handling error events e.g. connection failures or all 
        ///     brokers down. Note that the client will try to automatically 
        ///     recover from errors - these errors should be seen as 
        ///     informational rather than catastrophic.
        /// </summary>
        /// <remarks>
        ///     On the Consumer, executes as a side-effect of 
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Consume(System.Threading.CancellationToken)" />
        ///     (on the same thread) and on the Producer and AdminClient, on the
        ///     background poll thread.
        /// </remarks>
        public const string ErrorDelegateName = "error_cb";

        /// <summary>
        ///     Name of the configuration property that specifies a delegate for
        ///     handling statistics events - a JSON formatted
        ///     string as defined here: https://github.com/edenhill/librdkafka/wiki/Statistics
        /// </summary>
        /// <remarks>
        ///     You can enable statistics and set the statistics interval
        ///     using the statistics.interval.ms configuration parameter
        ///     (disabled by default).
        /// 
        ///     On the Consumer, executes as a side-effect of 
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Consume(System.Threading.CancellationToken)" />
        ///     (on the same thread) and on the Producer and AdminClient, on the
        ///     background poll thread.
        /// </remarks>
        public const string StatsDelegateName = "stats_cb";


        // ---- Deserializers

        /// <summary>
        ///     Name of the configuration parameter used to specify the encoding
        ///     when deserializing string keys.
        /// </summary>
        public const string DeserializerKeyEncodingConfigParam = "dotnet.string.deserializer.encoding.key";

        /// <summary>
        ///     Name of the configuration parameter used to specify the encoding
        ///     when deserializing string values.
        /// </summary>
        public const string DeserializerValueEncodingConfigParam = "dotnet.string.deserializer.encoding.value";


        // ---- Serializers

        /// <summary>
        ///     Name of the configuration parameter used to specify the encoding 
        ///     when serializing string keys.
        /// </summary>
        public const string SerializerKeyEncodingConfigParam = "dotnet.string.serializer.encoding.key";

        /// <summary>
        ///     Name of the configuration parameter used to specify the encoding 
        ///     when serializing string values.
        /// </summary>
        public const string SerializerValueEncodingConfigParam = "dotnet.string.serializer.encoding.value";


        /// <summary>
        ///     Name of the configuration parameter used to specify the initial size 
        ///     (in bytes) of the buffer used for Avro message serialization. Use a value 
        ///     high enough to avoid resizing the buffer, but small enough to avoid 
        ///     excessive memory use. Inspect the size of the byte array returned by the
        ///     Serialize method to estimate an appropriate value. Note: each call to 
        ///     serialize creates a new buffer.
        /// 
        ///     default: 1024
        /// </summary>
        public const string InitialBufferSizePropertyName = "avro.serializer.buffer.bytes";

        /// <summary>
        ///     Name of the configuration parameter used to specify whether or not the
        ///     Avro serializer should attempt to auto-register unrecognized schemas with
        ///     Confluent Schema Registry.
        ///
        ///     default: true
        /// </summary>
        public const string AutoRegisterSchemaPropertyName = "avro.serializer.auto.register.schemas";


        // ---- Schema Registry Client

        /// <summary>
        ///     Name of the configuration parameter used to specify a comma-separated list
        ///     of URLs for schema registry instances that are used to register or lookup
        ///     schemas.
        /// </summary>
        public const string SchemaRegistryUrlPropertyName = "schema.registry.url";

        /// <summary>
        ///     Name of the configuration parameter used to specify the timeout for requests
        ///     to Confluent Schema Registry.
        /// 
        ///     default: 30000
        /// </summary>
        public const string SchemaRegistryConnectionTimeoutMsPropertyName = "schema.registry.connection.timeout.ms";

        /// <summary>
        ///     Name of the configuration parameter used to specify the maximum number of
        ///     schemas CachedSchemaRegistryClient should cache locally.
        /// 
        ///     default: 1000
        /// </summary>
        public const string SchemaRegistryMaxCachedSchemasPropertyName = "schema.registry.max.cached.schemas";
    }
}