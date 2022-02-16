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

using System.Text;
using System.Linq;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Metadata pertaining to a single Kafka topic partition.
    /// </summary>
    public class PartitionMetadata
    {
        /// <summary>
        ///     Initializes a new PartitionMetadata instance.
        /// </summary>
        /// <param name="partitionId">
        ///     The id of the partition this metadata relates to.
        /// </param>
        /// <param name="leader">
        ///     The id of the broker that is the leader for the partition.
        /// </param>
        /// <param name="replicas">
        ///     The ids of all brokers that contain replicas of the partition.
        /// </param>
        /// <param name="inSyncReplicas">
        ///     The ids of all brokers that contain in-sync replicas of the partition.
        ///     Note: this value is cached by the broker and is consequently not guaranteed to be up-to-date.
        /// </param>
        /// <param name="error">
        ///     A rich <see cref="Error"/> object associated with the request for this partition metadata.
        /// </param>
        public PartitionMetadata(int partitionId, int leader, int[] replicas, int[] inSyncReplicas, Error error)
        {
            PartitionId = partitionId;
            Leader = leader;
            Replicas = replicas;
            InSyncReplicas = inSyncReplicas;
            Error = error;
        }

        /// <summary>
        ///     Gets the id of the partition this metadata relates to.
        /// </summary>
        public int PartitionId { get; }

        /// <summary>
        ///     Gets the id of the broker that is the leader for the partition.
        /// </summary>
        public int Leader { get; }

        /// <summary>
        ///     Gets the ids of all brokers that contain replicas of the partition.
        /// </summary>
        public int[] Replicas { get; }

        /// <summary>
        ///     Gets the ids of all brokers that contain in-sync replicas of the partition.
        /// </summary>
        public int[] InSyncReplicas { get; }

        /// <summary>
        ///     Gets a rich <see cref="Error"/> object associated with the request for this partition metadata.
        ///     Note: this value is cached by the broker and is consequently not guaranteed to be up-to-date.
        /// </summary>
        public Error Error { get; }

        /// <summary>
        ///     Returns a JSON representation of the PartitionMetadata object.
        /// </summary>
        /// <returns>
        ///     A JSON representation the PartitionMetadata object.
        /// </returns>
        public override string ToString()
        {
            var result = new StringBuilder();
            result.Append($"{{ \"PartitionId\": {PartitionId}, \"Leader\": {Leader}, \"Replicas\": [");
            result.Append(string.Join(",", Replicas.Select(r => $" {r.ToString()}")));
            result.Append(" ], \"InSyncReplicas\": [");
            result.Append(string.Join(",", InSyncReplicas.Select(r => $" {r.ToString()}")));
            result.Append($" ], \"Error\": \"{Error.Code.ToString()}\" }}");
            return result.ToString();
        }
    }

}