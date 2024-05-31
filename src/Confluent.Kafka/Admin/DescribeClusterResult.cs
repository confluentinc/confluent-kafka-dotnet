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
    ///     Represents the result of a describe cluster operation.
    /// </summary>
    public class DescribeClusterResult
    {
        /// <summary>
        ///     Current cluster Id (null if not supported).
        /// </summary>
        public string ClusterId { get; set; }

        /// <summary>
        ///     Current controller (null if not known).
        /// </summary>
        public Node Controller { get; set; }

        /// <summary>
        ///     Nodes in the cluster.
        /// </summary>
        public List<Node> Nodes { get; set; }

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
            var nodes = string.Join(",",
                Nodes.Select(node =>
                    node?.ToString() ?? "null"
                ).ToList());

            var authorizedOperations = "null";
            if (AuthorizedOperations != null)
            {
                authorizedOperations = string.Join(",",
                    AuthorizedOperations.Select(authorizedOperation =>
                        authorizedOperation.ToString().Quote()
                    ).ToList());
                authorizedOperations = $"[{authorizedOperations}]";
            }
            
            result.Append($"{{\"ClusterId\": {ClusterId.Quote()}");
            result.Append($", \"Controller\": {Controller?.ToString() ?? "null"}, \"Nodes\": [{nodes}]");
            result.Append($", \"AuthorizedOperations\": {authorizedOperations}}}");

            return result.ToString();
        }
    }
}
