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
    ///     Enumerates the different consumer group states.
    /// </summary>
    public enum ConsumerGroupState : int
    {
        /// <summary>
        ///     Unknown
        /// </summary>
        Unknown = 0,

        /// <summary>
        ///     Preparing rebalance
        /// </summary>
        PreparingRebalance = 1,

        /// <summary>
        ///     Completing rebalance
        /// </summary>
        CompletingRebalance = 2,

        /// <summary>
        ///     Stable state
        /// </summary>
        Stable = 3,

        /// <summary>
        ///     Dead
        /// </summary>
        Dead = 4,

        /// <summary>
        ///     Empty
        /// </summary>
        Empty = 5,
    };
}
