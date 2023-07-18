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
    ///     Represents the enum for different OffsetSpec which defines 
    ///     the offset to be fetched as per the OffsetSpec
    ///     To fetch for a particular timestamp, instead of OffsetSpec use the epoch Timestamp(long).
    /// </summary>
    public enum OffsetSpec : int
    {
        /// <summary>
        ///     MaxTimestamp
        /// </summary>
        MaxTimestamp = -3,

        /// <summary>
        ///     Latest
        /// </summary>
        Latest = -2,

        /// <summary>
        ///     Earliest
        /// </summary>
        Earliest = -1,
    }
}
