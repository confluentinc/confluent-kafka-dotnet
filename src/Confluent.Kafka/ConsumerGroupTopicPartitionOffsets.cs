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
    ///     Represents a Kafka tuple (consumer group, list of TopicPartitionOffsets).
    /// </summary>
    public class ConsumerGroupTopicPartitionOffsets
    {
        /// <summary>
        ///     Initializes a new ConsumerGroupTopicPartitionOffsets instance.
        /// </summary>
        /// <param name="group">
        ///     Kafka consumer group ID.
        /// </param>
        /// <param name="topicPartitionOffsets">
        ///     A list of Kafka (topic, partition) tuples.
        /// </param>
        public ConsumerGroupTopicPartitionOffsets(string group, List<TopicPartitionOffset> topicPartitionOffsets) {
            this.TopicPartitionOffsets = topicPartitionOffsets;
            this.Group = group;
        }

        /// <summary>
        ///     Gets the list of Kafka (topic, partition) tuples.
        /// </summary>
        public List<TopicPartitionOffset> TopicPartitionOffsets { get; }

        /// <summary>
        ///     Gets the Kafka consumer group ID.
        /// </summary>
        public string Group { get; }

        /// <summary>
        ///     Returns a string representation of the ConsumerGroupTopicPartitionOffsets object.
        /// </summary>
        /// <returns>
        ///     A string that represents the ConsumerGroupTopicPartitionOffsets object.
        /// </returns>
        public override string ToString()
            => $"{Group} [{TopicPartitionOffsets}]";
    }
}
