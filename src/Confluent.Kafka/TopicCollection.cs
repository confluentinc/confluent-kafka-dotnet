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


namespace Confluent.Kafka
{
    /// <summary>
    ///     A class used to represent a collection of topics.
    /// </summary>
    public class TopicCollection
    {
        /// <summary>
        ///    Avoid direct instantiation.
        /// </summary>
        private TopicCollection()
        {
        }
        
        /// <summary>
        ///     Topic names.
        /// </summary>
        internal IEnumerable<string> Topics { get; set; }

        /// <summary>
        ///    Returns a human readable representation of this object.
        /// </summary>
        public static TopicCollection OfTopicNames(IEnumerable<string> topics)
        {
            return new TopicCollection
            {
                Topics = topics
            };
        }

        /// <summary>
        ///     Returns a JSON representation of this object.
        /// </summary>
        /// <returns>
        ///     A JSON representation of this object.
        /// </returns>
        public override string ToString()
        {
            var result = new StringBuilder();
            var topics = string.Join(",",
                Topics.Select(topic => topic.Quote()).ToList());
            result.Append($"{{\"Topics\": [{topics}]}}");
            return result.ToString();
        }
    }
}
