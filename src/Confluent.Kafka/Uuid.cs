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
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using System;
using Confluent.Kafka.Impl;
using static Confluent.Kafka.Internal.Util.Marshal;

namespace Confluent.Kafka
{
    /// <summary>
    ///     Represents a UUID
    /// </summary>
    public class Uuid
    {
        /// <summary>
        ///     Initializes a new instance of the Uuid class
        ///     with the specified Most and Least significant bits.
        /// </summary>
        /// <param name="mostSignificantBits">
        ///     Most significant 64 bits of the 128 bits UUID.
        /// </param>
        /// <param name="leastSignificantBits">
        ///     Least significant 64 bits of the 128 bits UUID.
        /// </param>
        public Uuid(long mostSignificantBits, long leastSignificantBits)
        {
            Librdkafka.Initialize(null);
            MostSignificantBits = mostSignificantBits;
            LeastSignificantBits = leastSignificantBits;
            IntPtr cUuid = Librdkafka.Uuid_new(mostSignificantBits, leastSignificantBits);
            IntPtr cBase64str = Librdkafka.Uuid_base64str(cUuid);
            Base64str = PtrToStringUTF8(cBase64str);
            Librdkafka.Uuid_destroy(cUuid);
        }

        /// <summary>
        ///     Most significant 64 bits of the 128 bits UUID.
        /// </summary>
        public Offset MostSignificantBits { get; }

        /// <summary>
        ///     Most significant 64 bits of the 128 bits UUID.
        /// </summary>
        public Offset LeastSignificantBits { get; }

        private readonly string Base64str;

        /// <summary>
        ///     Returns a string representation of the Uuid object.
        /// </summary>
        /// <returns>
        ///     A string representation of the Uuid object.
        /// </returns>
        public override string ToString()
            => Base64str;
    }
}
