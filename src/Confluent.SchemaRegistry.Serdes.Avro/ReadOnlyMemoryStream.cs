// Copyright 2025 Confluent Inc.
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
using System.IO;

namespace Confluent.SchemaRegistry.Serdes;

/// <summary>
/// A read-only <see cref="Stream"/> implementation over a <see cref="ReadOnlyMemory{Byte}"/>.
/// This class is needed to avoid calling <c>ToArray()</c> on <see cref="ReadOnlyMemory{Byte}"/>
/// when passing data to APIs (such as Avro's <c>BinaryDecoder</c>) that require a <see cref="Stream"/>.
/// </summary>
internal class ReadOnlyMemoryStream : Stream
{
    private readonly ReadOnlyMemory<byte> data;

    public ReadOnlyMemoryStream(ReadOnlyMemory<byte> data)
    {
        this.data = data;
    }

    public override void Flush()
    {
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        if (buffer == null)
            throw new ArgumentNullException(nameof(buffer));
        if (offset < 0 || count < 0 || offset + count > buffer.Length)
            throw new ArgumentOutOfRangeException();

        var remaining = data.Length - (int) Position;
        if (remaining <= 0)
            return 0;

        var toRead = Math.Min(count, remaining);
        data.Slice((int) Position, toRead).Span.CopyTo(buffer.AsSpan(offset, toRead));
        Position += toRead;
        return toRead;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        switch (origin)
        {
            case SeekOrigin.Begin:
                Position = offset;
                break;
            case SeekOrigin.Current:
                Position += offset;
                break;
            case SeekOrigin.End:
                Position = Length + offset;
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(origin), origin, null);
        }

        if (Position < 0 || Position > Length)
            throw new IOException("Seek operation resulted in an invalid position.");

        return Position;
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException();
    }

    public override bool CanRead => true;
    public override bool CanSeek => true;
    public override bool CanWrite => false;
    public override long Length => data.Length;
    public override long Position { get; set; }
}