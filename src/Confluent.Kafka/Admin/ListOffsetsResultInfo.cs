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

using System.Text;


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     Represents the result of a list offset request for a single topic partition.
    /// </summary>
    public class ListOffsetsResultInfo
    {
        /// <summary>
        ///    TopicPartitionOffsetError corresponding to the requested TopicPartition.
        /// </summary>
        public TopicPartitionOffsetError TopicPartitionOffsetError { get; set; }

        /// <summary>
        ///    Timestamp corresponding to the offset, -1 if not set by the broker.
        /// </summary>
        public long Timestamp { get; set; }

        /// <summary>
        ///     Returns a JSON representation of the object.
        /// </summary>
        /// <returns>
        ///     A JSON representation of the object.
        /// </returns>
        public override string ToString()
        {
            var result = new StringBuilder();
            result.Append($"{{\"TopicPartitionOffsetError\": {TopicPartitionOffsetError.ToString().Quote()}");
            result.Append($", \"Timestamp\": {Timestamp}");
            result.Append("}");
            return result.ToString();
        }
    }
}
