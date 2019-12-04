//    Copyright 2019 Chad Jessup
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// 
// 
// Derived from: rdkafka-dotnet - C# Apache Kafka client library
// 
// Copyright (c) 2015-2016, Andreas Heider
// All rights reserved.
// 
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
// 
// 1. Redistributions of source code must retain the above copyright notice,
//    this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
// 
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED.IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

namespace Confluent.Kafka
{
    /// <summary>
    ///     Request to retrieve the partition to send a message to.
    /// </summary>
    /// <remarks>
    ///     A request for the partition is only generated if the <seealso cref="Partition"/> is set to <seealso cref="Partition.Any"/>.
    /// </remarks>
    /// <typeparam name="TKey">The type of the Key.</typeparam>
    /// <typeparam name="TValue">The type of the Value.</typeparam>
    public class PartitionRequest<TKey, TValue>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="PartitionRequest{TKey, TValue}"/> class.
        /// </summary>
        /// <param name="message">The message to determine the <seealso cref="Partition"/> for.</param>
        /// <param name="key">The key in the message.</param>
        /// <param name="keyBytes">The bytes of the key after serialization.</param>
        /// <param name="value">The value in the message.</param>
        /// <param name="valueBytes">The bytes of the value after serialization.</param>
        public PartitionRequest(Message<TKey, TValue> message, TKey key, byte[] keyBytes, TValue value, byte[] valueBytes)
        {
            this.Message = message;

            this.Key = key;
            this.KeyBytes = keyBytes;

            this.Value = value;
            this.ValueBytes = valueBytes;
        }

        /// <summary>
        ///     Gets the message that needs a <seealso cref="Partition"/> determined for.
        /// </summary>
        public Message<TKey, TValue> Message { get; }

        /// <summary>
        ///     Gets the key.
        /// </summary>
        public TKey Key { get; }

        /// <summary>
        ///     Gets the bytes of the key after serialization.
        /// </summary>
        public byte[] KeyBytes { get; }

        /// <summary>
        ///     Gets the value.
        /// </summary>
        public TValue Value { get; }

        /// <summary>
        ///     Gets the bytes of the value after serialization.
        /// </summary>
        public byte[] ValueBytes { get; }
    }
}
