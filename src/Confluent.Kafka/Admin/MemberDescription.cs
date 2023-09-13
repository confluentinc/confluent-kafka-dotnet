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

using System.Linq;
using System.Text;


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     MemberDescription represents the description of a consumer group member
    /// </summary>
    public class MemberDescription
    {
        /// <summary>
        ///     Client id.
        /// </summary>
        public string ClientId { get; set; }

        /// <summary>
        ///     Group instance id.
        /// </summary>
        public string GroupInstanceId { get; set; }

        /// <summary>
        ///     Consumer id.
        /// </summary>
        public string ConsumerId { get; set; }

        /// <summary>
        ///     Group member host.
        /// </summary>
        public string Host { get; set; }

        /// <summary>
        ///     Member assignment.
        /// </summary>
        public MemberAssignment Assignment { get; set; }
        
        /// <summary>
        ///     Returns a JSON representation of this object.
        /// </summary>
        /// <returns>
        ///     A JSON representation of this object.
        /// </returns>
        public override string ToString()
        {
            var result = new StringBuilder();
            var assignment = string.Join(",",
                Assignment.TopicPartitions.Select(topicPartition => 
                    $"{{\"Topic\": {topicPartition.Topic.Quote()}, \"Partition\": {topicPartition.Partition.Value}}}"
                ).ToList());
            
            result.Append($"{{\"ClientId\": {ClientId.Quote()}");
            result.Append($", \"GroupInstanceId\": {GroupInstanceId.Quote()}, \"ConsumerId\": {ConsumerId.Quote()}");
            result.Append($", \"Host\": {Host.Quote()}, \"Assignment\": [{assignment}]}}");

            return result.ToString();
        }
    }
}
