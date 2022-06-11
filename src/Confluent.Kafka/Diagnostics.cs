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
using System.Runtime.InteropServices;

namespace Confluent.Kafka
{
    internal static class Diagnostics
    {
        public const string ActivitySourceName = "Confluent.Kafka";
        public static ActivitySource ActivitySource { get; } = new ActivitySource(ActivitySourceName);

        public static class Producer
        {
            public const string ActivityName = ActivitySourceName + ".MessageProduced";

            public static Activity Start<TKey, TValue>(TopicPartition topicPartition, Message<TKey, TValue> message)
            {
                Activity activity = ActivitySource.StartActivity(ActivityName);

                if (activity == null)
                    return null;

                using (activity)
                {
                    activity?.AddDefaultOpenTelemetryTags(topicPartition, message);
                    activity?.AddTag("messaging.kafka.partition", topicPartition.Partition.Value);
                }

                return activity;
            }
        }

        private static Activity AddDefaultOpenTelemetryTags<TKey, TValue>(
            this Activity activity,
            TopicPartition topicPartition,
            Message<TKey, TValue> message)
        {
            activity?.AddTag("messaging.system", "kafka");
            activity?.AddTag("messaging.destination", topicPartition.Topic);
            activity?.AddTag("messaging.destination_kind", "topic");
            activity?.AddTag("messaging.kafka.partition", topicPartition.Partition.Value);

            if (message.Key != null)
                activity?.AddTag("messaging.kafka.message_key", message.Key);

            if (message.Value != null)
                activity?.AddTag("messaging.message_payload_size_bytes", Marshal.SizeOf(message.Value));

            return activity;
        }
    }
}
