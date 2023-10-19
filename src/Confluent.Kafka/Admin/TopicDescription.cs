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
using System.Linq;
using System.Text;


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     Represents a single topic's description in the result of a
    ///     describe topic operation.
    /// </summary>
    public class TopicDescription
    {
        /// <summary>
        ///     The topic name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        ///     The topic Id.
        /// </summary>
        public Uuid TopicId {get; set; }

        /// <summary>
        ///     Error, if any, of topic reported by the broker
        /// </summary>
        public Error Error { get; set; }
        
        /// <summary>
        ///      Whether the topic is internal to Kafka.
        ///      An example of an internal topic is the offsets and group management topic: __consumer_offsets.
        /// </summary>
        public bool IsInternal { get; set; }

        /// <summary>
        ///    List of partitions and their information.
        /// </summary>
        public List<TopicPartitionInfo> Partitions { get; set; }

        /// <summary>
        ///    AclOperation list (null if not requested or not supported).
        /// </summary>
        public List<AclOperation> AuthorizedOperations { get; set; }
        
        /// <summary>
        ///     Returns a JSON representation of this object.
        /// </summary>
        /// <returns>
        ///     A JSON representation of this object.
        /// </returns>
        public override string ToString()
        {
            var result = new StringBuilder();
            var partitions = string.Join(",",
                Partitions.Select(partition =>
                    partition.ToString()).ToList());

            var authorizedOperations = "null";
            if (AuthorizedOperations != null)
            {
                authorizedOperations = string.Join(",",
                    AuthorizedOperations.Select(authorizedOperation =>
                        authorizedOperation.ToString().Quote()
                    ).ToList());
                authorizedOperations = $"[{authorizedOperations}]";
            }

            result.Append($"{{\"Name\": {Name.Quote()}");
            if (TopicId != null)
            {
                result.Append($", \"TopicId\": {TopicId.ToString().Quote()}");
            }
            result.Append($", \"Error\": \"{Error.Code}\", \"IsInternal\": {IsInternal.Quote()}");
            result.Append($", \"Partitions\": [{partitions}], \"AuthorizedOperations\": {authorizedOperations}}}");
            return result.ToString();
        }
    }
}
