// Copyright 2018 Confluent Inc.
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


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     Enumerates the set of configuration resource types.
    /// </summary>
    public enum ResourceType : int
    {
        /// <summary>
        ///     Unknown resource
        /// </summary>
        Unknown = 0,

        /// <summary>
        ///     Any resource
        /// </summary>
        Any = 1,

        /// <summary>
        ///     Topic resource
        /// </summary>
        Topic = 2,

        /// <summary>
        ///     Group resource
        /// </summary>
        Group = 3,

        /// <summary>
        ///     Broker resource
        /// </summary>
        Broker = 4
    }
}
