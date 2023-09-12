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


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     Represents the result of a describe cluster operation.
    /// </summary>
    public class DescribeClusterResult
    {
        /// <summary>
        ///     Current cluster Id.
        /// </summary>
        public string ClusterId { get; set; }

        /// <summary>
        ///     Current controller (optional).
        /// </summary>
        public Node Controller { get; set; }

        /// <summary>
        ///     Nodes in the cluster.
        /// </summary>
        public List<Node> Nodes { get; set; }

        /// <summary>
        ///    AclOperation list.
        /// </summary>
        public List<AclOperation> AuthorizedOperations { get; set; }


        /// <summary>
        ///    Returns a human readable representation of this object.
        /// </summary>
        public override string ToString() {
            string res = "ClusterId: ";
            res += ClusterId + "\n";
            res += "ControllerId: " + Controller + "\n";
            res += "Nodes:\n";
            foreach (Node node in Nodes) {
                res += "\t" + node.ToString() + "\n";
            }
            return res;
        }
    }
}
