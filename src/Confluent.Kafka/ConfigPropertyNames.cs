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

    }
}