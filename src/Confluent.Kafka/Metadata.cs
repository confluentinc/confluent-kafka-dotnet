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
using System.Text;
using System.Linq;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Kafka cluster metadata.
    /// </summary>
    public class Metadata
    {
        /// <summary>
        ///     Instantiates a new Metadata class instance.
        /// </summary>
        /// <param name="brokers">
        ///     Information about each constituent broker of the cluster.
        /// </param>
        /// <param name="topics">
        ///     Information about requested topics in the cluster.
        /// </param>
        /// <param name="originatingBrokerId">
        ///     The id of the broker that provided this metadata.
        /// </param>
        /// <param name="originatingBrokerName">
        ///     The name of the broker that provided this metadata.
        /// </param>
        public Metadata(List<BrokerMetadata> brokers, List<TopicMetadata> topics, int originatingBrokerId, string originatingBrokerName)
        {
            Brokers = brokers;
            Topics = topics;
            OriginatingBrokerId = originatingBrokerId;
            OriginatingBrokerName = originatingBrokerName;
        }

        /// <summary>
        ///     Gets information about each constituent broker of the cluster.
        /// </summary>
        public List<BrokerMetadata> Brokers { get; }

        /// <summary>
        ///     Gets information about requested topics in the cluster.
        /// </summary>
        public List<TopicMetadata> Topics { get; }

        /// <summary>
        ///     Gets the id of the broker that provided this metadata.
        /// </summary>
        public int OriginatingBrokerId { get; }

        /// <summary>
        ///     Gets the name of the broker that provided this metadata.
        /// </summary>
        public string OriginatingBrokerName { get; }

        /// <summary>
        ///     Returns a JSON representation of the Metadata object.
        /// </summary>
        /// <returns>
        ///     A JSON representation of the Metadata object.
        /// </returns>
        public override string ToString()
        {
            var result = new StringBuilder();
            result.Append($"{{ \"OriginatingBrokerId\": {OriginatingBrokerId}, \"OriginatingBrokerName\": \"{OriginatingBrokerName}\", \"Brokers\": [");
            result.Append(string.Join(",", Brokers.Select(b => $" {b.ToString()}")));
            result.Append($" ], \"Topics\": [");
            result.Append(string.Join(",", Topics.Select(t => $" {t.ToString()}")));
            result.Append("] }");
            return result.ToString();
        }
    }
}
