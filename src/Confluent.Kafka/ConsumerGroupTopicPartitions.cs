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

using System.Collections.Generic;

namespace Confluent.Kafka
{
    /// <summary>
    ///     Represents a Kafka tuple (consumer group, list of TopicPartions).
    /// </summary>
    public class ConsumerGroupTopicPartitions
    {
        /// <summary>
        ///     Initializes a new ConsumerGroupTopicPartitions instance.
        /// </summary>
        /// <param name="group">
        ///     Kafka consumer group ID.
        /// </param>
        /// <param name="topicPartitions">
        ///     A list of Kafka (topic, partition) tuples.
        /// </param>
        public ConsumerGroupTopicPartitions(string group, List<TopicPartition> topicPartitions) {
            this.TopicPartitions = topicPartitions;
            this.Group = group;
        }

        /// <summary>
        ///     Gets the list of Kafka (topic, partition) tuples.
        /// </summary>
        public List<TopicPartition> TopicPartitions { get; }

        /// <summary>
        ///     Gets the Kafka consumer group ID.
        /// </summary>
        public string Group { get; }

        /// <summary>
        ///     Returns a string representation of the ConsumerGroupTopicPartitions object.
        /// </summary>
        /// <returns>
        ///     A string that represents the ConsumerGroupTopicPartitions object.
        /// </returns>
        public override string ToString()
            => $"{Group} [{TopicPartitions}]";
    }
}
