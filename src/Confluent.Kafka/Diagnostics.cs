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

using System.Diagnostics;

namespace Confluent.Kafka
{
    /// <summary>
    /// Implements Activity objects with OpenTelemetry messaging tags for instrumentation
    /// </summary>
    internal static class Diagnostics
    {
        private const string ActivitySourceName = "Confluent.Kafka";
        public static ActivitySource ActivitySource { get; } = new ActivitySource(ActivitySourceName);

        /// <summary>
        /// Provides an Activity object for the Producer with OpenTelemetry messaging tags for instrumentation
        /// </summary>
        internal static class Producer
        {
            private const string ActivityName = ActivitySourceName + ".MessageProduced";

            internal static Activity Start<TKey, TValue>(TopicPartition topicPartition, Message<TKey, TValue> message)
            {
                Activity activity = ActivitySource.StartActivity(ActivityName);

                if (activity == null)
                    return null;

                using (activity)
                {
                    activity?.AddDefaultOpenTelemetryTags(topicPartition, message);
                }

                return activity;
            }
        }

        private static Activity AddDefaultOpenTelemetryTags<TKey, TValue>(
            this Activity activity,
            TopicPartition topicPartition,
            Message<TKey, TValue> message)
        {
            activity?.AddTag(OpenTelemetryMessaging.SYSTEM, "kafka");
            activity?.AddTag(OpenTelemetryMessaging.DESTINATION, topicPartition.Topic);
            activity?.AddTag(OpenTelemetryMessaging.DESTINATION_KIND, "topic");
            activity?.AddTag(OpenTelemetryMessaging.KAFKA_PARTITION, topicPartition.Partition.Value.ToString());

            if (message.Key != null)
                activity?.AddTag(OpenTelemetryMessaging.KAFKA_MESSAGE_KEY, message.Key);

            return activity;
        }
    }
}
