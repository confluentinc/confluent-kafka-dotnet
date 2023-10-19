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
    ///     Represents the result of a list offset request for a single topic partition.
    /// </summary>
    public class ListOffsetsResultInfo
    {
        /// <summary>
        ///    TopicPartitionOffsetError of the ListOffset Result Element which is a Topic Partition
        /// </summary>
        public TopicPartitionOffsetError TopicPartitionOffsetError { get; set; }

        /// <summary>
        ///    Timestamp Corresponding to the Offset, -1 if not set by broker.
        /// </summary>
        public long Timestamp { get; set; }

        /// <summary>
        ///    Returns a human readable representation of this object.
        /// </summary>
        public override string ToString()
        {
            string res = "TopicPartitionOffsetError:\n";
            res += TopicPartitionOffsetError.ToString();
            res += "\n";
            res += $"Timestamp : {Timestamp}\n";
            return res;
        }
    }
}
