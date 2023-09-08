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

using System.Collections.Generic;

namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     Represents the cluster's description in the result of a
    ///     describe cluster operation.
    /// </summary>
    public class ClusterDescription
    {
        /// <summary>
        ///     The current cluster Id.
        /// </summary>
        public string ClusterId { get; set; }

        /// <summary>
        ///     The current controller Id.
        /// </summary>
        public int ControllerId { get; set; }

        /// <summary>
        ///     The broker nodes in the cluster.
        /// </summary>
        public List<Node> Nodes{ get; set; }

        /// <summary>
        ///    AclOperation list.
        /// </summary>
        public List<AclOperation> AuthorizedOperations { get; set; }

    }
}
