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


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     Represents a consumer group id with the partitions for which the offsets need to 
    ///     be reset for. Used by "IAdminClient.DeleteConsumerGroupOffsetsAsync".
    /// </summary>
    public class ConsumerGroupOffsets
    {
        /// <summary>
        ///     Initializes a new ConsumerGroupOffsets instance.
        /// </summary>
        /// <param name="group">
        ///     Consumer group id.
        /// </param>
        /// <param name="partitions">
        ///     Partitions for which the offsets need to be reset for.
        /// </param>
        public ConsumerGroupOffsets(string group, List<TopicPartition> partitions)
        {
            Group = group;
            Partitions = partitions;
        }

        /// <summary>
        ///    Consumer group id.
        /// </summary>
        public string Group { get; set; }

        /// <summary>
        ///     Partitions for which the offsets were reset for.
        /// </summary>
        public List<TopicPartition> Partitions { get; set; }
    }
}
