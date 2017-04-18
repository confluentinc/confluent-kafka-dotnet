// Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider
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
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using System.Collections.Generic;
using System.Linq;
using System.Text;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Metadata pertaining to a single Kafka topic.
    /// </summary>
    public class TopicMetadata
    {
        /// <summary>
        ///     Initializes a new TopicMetadata class instance.
        /// </summary>
        /// <param name="topic">
        ///     The topic name.
        /// </param>
        /// <param name="partitions">
        ///     Metadata for each of the topic's partitions.
        /// </param>
        /// <param name="error">
        ///     A rich <see cref="Error"/> object associated with the request for this topic metadata.
        /// </param>
        public TopicMetadata(string topic, List<PartitionMetadata> partitions, Error error)
        {
            Topic = topic;
            Error = error;
            Partitions = partitions;
        }

        /// <summary>
        ///     Gets the topic name.
        /// </summary>
        public string Topic { get; }

        /// <summary>
        ///     Gets metadata for each of the topics partitions.
        /// </summary>
        public List<PartitionMetadata> Partitions { get; }

        /// <summary>
        ///     A rich <see cref="Error"/> object associated with the request for this topic metadata.
        /// </summary>
        public Error Error { get; }

        /// <summary>
        ///     Returns a JSON representation of the TopicMetadata object.
        /// </summary>
        /// <returns>
        ///     A JSON representation the TopicMetadata object.
        /// </returns>
        public override string ToString()
        {
            var result = new StringBuilder();
            result.Append($"{{ \"Topic\": \"{Topic}\", \"Partitions\": [");
            result.Append(string.Join(",", Partitions.Select(p => $" {p.ToString()}")));
            result.Append($" ], \"Error\": \"{Error.Code.ToString()}\" }}");
            return result.ToString();
        }
    }
}
