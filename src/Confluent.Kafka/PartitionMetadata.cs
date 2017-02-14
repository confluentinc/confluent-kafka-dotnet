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
        public PartitionMetadata(int partitionId, int leader, int[] replicas, int[] inSyncReplicas, Error error)
        {
            PartitionId = partitionId;
            Leader = leader;
            Replicas = replicas;
            InSyncReplicas = inSyncReplicas;
            Error = error;
        }

        public int PartitionId { get; }
        public int Leader { get; }
        public int[] Replicas { get; }
        public int[] InSyncReplicas { get; }
        public Error Error { get; }

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