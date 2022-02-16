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
using System.Threading;
using System.Threading.Tasks;


namespace Confluent.Kafka
{
    /// <summary>
    /// A <see cref="Stream"/> implementation wrapping an <see cref="IBufferWriter{Byte}"/> instance.
    /// </summary>
    internal sealed class BufferWriterStream : Stream
    {
        private readonly IBufferWriter<byte> bufferWriter;
        private bool disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="BufferWriterStream"/> class.
        /// </summary>
        /// <param name="bufferWriter">The target <see cref="IBufferWriter{Byte}"/> instance to use.</param>
        public BufferWriterStream(IBufferWriter<byte> bufferWriter)
        {
            this.bufferWriter = bufferWriter ?? throw new ArgumentNullException(nameof(bufferWriter));
        }

        /// <inheritdoc/>
        public override bool CanRead => false;

        /// <inheritdoc/>
        public override bool CanSeek => false;

        /// <inheritdoc/>
        public override bool CanWrite
        {
            get => !this.disposed;
        }

        /// <inheritdoc/>
        public override long Length => throw new NotSupportedException();

        /// <inheritdoc/>
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        /// <inheritdoc/>
        public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc/>
        public override void Flush()
        {
        }

        /// <inheritdoc/>
        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(true);
        }

        /// <inheritdoc/>
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc/>
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Write(buffer, offset, count);
            cancellationToken.ThrowIfCancellationRequested();

            return Task.FromResult(true);
        }

        /// <inheritdoc/>
        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc/>
        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc/>
        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc/>
        public override int ReadByte()
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc/>
        public override void Write(byte[] buffer, int offset, int count)
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException(nameof(BufferWriterStream));
            }

            var source = buffer.AsSpan(offset, count);
            var destination = this.bufferWriter.GetSpan(count);

            source.CopyTo(destination);

            this.bufferWriter.Advance(count);
        }

        /// <inheritdoc/>
        public override void WriteByte(byte value)
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException(nameof(BufferWriterStream));
            }

            this.bufferWriter.GetSpan(1)[0] = value;

            this.bufferWriter.Advance(1);
        }

        /// <inheritdoc/>
        protected override void Dispose(bool disposing)
        {
            this.disposed = true;
        }
    }
}
