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


namespace Confluent.Kafka
{
    /// <summary>
    /// Implements a <see cref="IBufferWriter{Byte}"/> using <see cref="ArrayPool{Byte}.Shared"/> for memory allocation.
    /// </summary>
    internal sealed class ArrayPoolBufferWriter : ISerializationBuffer
    {
        /// <summary>
        /// The default buffer size to use to expand empty arrays.
        /// </summary>
        private const int DefaultInitialBufferSize = 256;

        private readonly ArrayPool<byte> pool;
        private int index;
        private byte[] array;

        /// <summary>
        /// Initializes a new instance of the <see cref="ArrayPoolBufferWriter"/> class.
        /// </summary>
        public ArrayPoolBufferWriter()
        {
            this.pool = ArrayPool<byte>.Shared;
            this.array = pool.Rent(DefaultInitialBufferSize);
            this.index = 0;
        }

        /// <summary>
        /// Returns the memory allocation on finialization if not explicitly disposed in user code.
        /// </summary>
        ~ArrayPoolBufferWriter() => Dispose();

        /// <inheritdoc />
        public void Advance(int count)
        {
            byte[] array = this.array;

            if (array is null)
            {
                throw new ObjectDisposedException(nameof(ArrayPoolBufferWriter));
            }

            if (count < 0)
            {
                throw new ArgumentOutOfRangeException("Count must be greater than 0");
            }

            if (this.index > array.Length - count)
            {
                throw new ArgumentOutOfRangeException("Cannot advance further than current capacity");
            }

            this.index += count;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (this.array != null)
            {
                this.pool.Return(this.array, true);
                this.array = null;
            }
        }

     
        public ArraySegment<byte> GetComitted(int offset = 0)
        {
            return new ArraySegment<byte>(this.array, offset, this.index);
        }

        /// <inheritdoc />
        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return this.array.AsMemory(this.index);
        }

        /// <inheritdoc />
        public Span<byte> GetSpan(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return this.array.AsSpan(this.index);
        }

        private void EnsureCapacity(int sizeHint)
        {
            var array = this.array;

            if (array is null)
            {
                throw new ObjectDisposedException(nameof(ArrayPoolBufferWriter));
            }

            if (sizeHint < 0)
            {
                throw new ArgumentOutOfRangeException("Cannot advance further than current capacity");
            }

            if (sizeHint == 0)
            {
                sizeHint = 1;
            }

            if (sizeHint > array.Length - this.index)
            {
                int minimumSize = this.index + sizeHint;
                var newArray = pool.Rent(minimumSize);

                Array.Copy(array, 0, newArray, 0, this.index);
                pool.Return(array, true);
                array = newArray;
            }
        }
    }
}