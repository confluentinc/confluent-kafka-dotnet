// Copyright 2022 Confluent Inc.
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
    /// Provides the OpenTelemetry messaging attributes.
    /// The complete list of messaging attributes specification is available here: https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md#messaging-attributes
    /// </summary>
    public static class OpenTelemetryMessaging
    {
        /// <summary>
        /// Message system. For Kafka, attribute value must be "kafka".
        /// </summary>
        public const string SYSTEM = "messaging.system";

        /// <summary>
        /// Message destination. For Kafka, attribute value must be a Kafka topic.
        /// </summary>
        public const string DESTINATION = "messaging.destination";

        /// <summary>
        /// Destination kind. For Kafka, attribute value must be "topic".
        /// </summary>
        public const string DESTINATION_KIND = "messaging.destination_kind";

        /// <summary>
        /// Kafka partition number.
        /// </summary>
        public const string KAFKA_PARTITION = "messaging.kafka.partition";

        /// <summary>
        /// Kafka message key.
        /// </summary>
        public const string KAFKA_MESSAGE_KEY = "messaging.kafka.message_key";
    }
}
