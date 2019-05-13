// Copyright 2018 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.


namespace Confluent.Kafka
{
    /// <summary>
    ///     Names of all configuration properties specific to the
    ///     .NET Client.
    /// </summary>
    public static class ConfigPropertyNames
    {
        /// <summary>
        ///     Producer specific configuration properties.
        /// </summary>
        public static class Producer
        {
            /// <summary>
            ///     Specifies whether or not the producer should start a background poll 
            ///     thread to receive delivery reports and event notifications. Generally,
            ///     this should be set to true. If set to false, you will need to call 
            ///     the Poll function manually.
            /// 
            ///     default: true
            /// </summary>
            public const string EnableBackgroundPoll = "dotnet.producer.enable.background.poll";

            /// <summary>
            ///     Specifies whether to enable notification of delivery reports. Typically
            ///     you should set this parameter to true. Set it to false for "fire and
            ///     forget" semantics and a small boost in performance.
            /// 
            ///     default: true
            /// </summary>
            public const string EnableDeliveryReports = "dotnet.producer.enable.delivery.reports";

            /// <summary>
            ///     A comma separated list of fields that may be optionally set in delivery
            ///     reports. Disabling delivery report fields that you do not require will
            ///     improve maximum throughput and reduce memory usage. Allowed values:
            ///     key, value, timestamp, headers, status, all, none.
            /// 
            ///     default: all
            /// </summary>
            public const string DeliveryReportFields = "dotnet.producer.delivery.report.fields";
        }
        

        /// <summary>
        ///     Consumer specific configuration properties.
        /// </summary>
        public static class Consumer
        {
            /// <summary>
            ///     A comma separated list of fields that may be optionally set
            ///     in <see cref="Confluent.Kafka.ConsumeResult{TKey,TValue}" />
            ///     objects returned by the
            ///     <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Consume(System.TimeSpan)" />
            ///     method. Disabling fields that you do not require will improve 
            ///     throughput and reduce memory consumption. Allowed values:
            ///     headers, timestamp, topic, all, none
            /// 
            ///     default: all
            /// </summary>
            public const string ConsumeResultFields = "dotnet.consumer.consume.result.fields";
        }

        /// <summary>
        ///     The maximum length of time (in milliseconds) before a cancellation request
        ///     is acted on. Low values may result in measurably higher CPU usage.
        ///     
        ///     default: 100
        ///     range: 1 &lt;= dotnet.cancellation.delay.max.ms &lt;= 10000
        /// </summary>
        public const string CancellationDelayMaxMs = "dotnet.cancellation.delay.max.ms";
        
    }
}
