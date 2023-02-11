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

namespace Confluent.Kafka
{
    /// <summary>
    ///     Node represents a Kafka broker.
    /// </summary>
    public class Node
    {
        /// <summary>
        ///     Id represents the Node Id.
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        ///     Host represents the host of the broker.
        /// </summary>
        public string Host { get; set; }

        /// <summary>
        ///     Port represents the port of the broker.
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        ///    Returns a human readable representation of this object.
        /// </summary>
        public override string ToString()
        {
            return $"Id = {Id}, {Host}:{Port}";
        }
    }
}