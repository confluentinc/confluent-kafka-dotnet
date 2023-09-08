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
    }
}
