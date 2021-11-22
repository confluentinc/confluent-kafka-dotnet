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

using System;
using System.Buffers;

namespace Confluent.Kafka
{
    /// <summary>
    /// Defines a buffer used to serialize keys and value. This buffer is disposed when no longer used.
    /// </summary>
    public interface ISerializationBuffer : IBufferWriter<byte>, IDisposable
    {
        /// <summary>
        ///     Gets an <see cref="ArraySegment{Byte}"/> representing the currently comitted memory.
        /// </summary>
        /// <param name="offset">
        ///     An optional offset into the comitted memory.
        /// </param>
        /// <returns>
        ///     Returns a <see cref="ArraySegment{Byte}"/> representing the commited memory with an initial offset as requested.
        /// </returns>
        ArraySegment<byte> GetComitted(int offset = 0);
    }
}
