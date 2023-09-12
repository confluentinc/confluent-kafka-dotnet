// Copyright 2023 Confluent Inc.
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
    ///     TopicPartitionInfo represents the description of a partition within a topic.
    ///     Used for result of DescribeTopics for an individual topic.
    /// </summary>
    public class TopicPartitionInfo
    {
        /// <summary>
        ///     Partition id.
        /// </summary>
        public int Partition { get; set; }

        /// <summary>
        ///     Leader broker.
        /// </summary>
        public Node Leader { get; set; }

        /// <summary>
        ///    Replica brokers list.
        /// </summary>
        public List<Node> Replicas { get; set; }

        /// <summary>
        ///    In-sync replica brokers list.
        /// </summary>
        public List<Node> ISR { get; set; }
    }
}