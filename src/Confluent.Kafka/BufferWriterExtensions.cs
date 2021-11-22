// Copyright 2016-2018 Confluent Inc.
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
using System.IO;


namespace Confluent.Kafka
{
    /// <summary>
    /// Extends a <see cref="IBufferWriter{Byte}"/> with a <see cref="Stream"/> adapter.
    /// </summary>
    public static class BufferWriterExtensions
    {
        /// <summary>
        /// Gets a <see cref="Stream"/> adapting implementation working on a <see cref="IBufferWriter{Byte}"/> as underlying memory.
        /// </summary>
        /// <param name="bufferWriter">The <see cref="IBufferWriter{Byte}"/> used for underlying memory.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">Thrown if the provises <paramref name="bufferWriter"/> is null.</exception>
        public static Stream AsStream(this IBufferWriter<byte> bufferWriter)
        {
            if (bufferWriter is null)
            {
                throw new ArgumentNullException(nameof(bufferWriter));
            }

            return new BufferWriterStream(bufferWriter);
        }
    }
}
