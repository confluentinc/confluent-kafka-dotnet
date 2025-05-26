// Copyright 2024 Confluent Inc.
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
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Confluent.Shared;

internal static class BinaryConverter
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int WriteInt32(Span<byte> destination, int value)
    {
        Unsafe.WriteUnaligned(ref MemoryMarshal.GetReference(destination), BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(value) : value);
        return sizeof(int);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int WriteByte(Span<byte> destination, byte value)
    {
        destination[0] = value;
        return sizeof(byte);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int WriteVarint(Span<byte> destination, uint value)
    {
        return WriteUnsignedVarint(destination, (value << 1) ^ (value >> 31));
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int WriteUnsignedVarint(Span<byte> destination, uint value)
    {
        int offset = 0;
        while ((value & 0xffffff80) != 0)
        {
            offset += WriteByte(destination.Slice(offset), (byte)((value & 0x7f) | 0x80));
            value >>= 7;
        }
        
        offset += WriteByte(destination.Slice(offset), (byte)value);

        return offset;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int WriteGuidBigEndian(Span<byte> destination, Guid value)
    {
#if NET8_0_OR_GREATER
        if (value.TryWriteBytes(destination, bigEndian: true, out var bytesWritten))
        {
            return bytesWritten;
        }
        else
        {
            throw new ArgumentException("Destination span is too small to write the Guid.", nameof(destination));
        }
#else
        var bytes = value.ToByteArray();
        if (BitConverter.IsLittleEndian)
        {
            Array.Reverse(bytes, 0, 4); // Reverse the first 4 bytes (int part)
            Array.Reverse(bytes, 4, 2); // Reverse the next 2 bytes (short part)
            Array.Reverse(bytes, 6, 2); // Reverse the last 2 bytes (short part)
        }
        bytes.CopyTo(destination);
        return 16;
#endif
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ReadByte(ReadOnlySpan<byte> source, out byte value)
    {
        value = source[0];
        return sizeof(byte);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ReadInt32(ReadOnlySpan<byte> source, out int value)
    {
        value = BinaryPrimitives.ReadInt32BigEndian(source);
        return sizeof(int);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ReadUnsignedVarint(ReadOnlySpan<byte> source, out uint value)
    {
        value = 0;
        int i = 0;
        int bytesRead = 0;

        for (var index = 0; index < source.Length; index++)
        {
            var b = source[index];
            bytesRead++;
            if ((b & 0x80) == 0)
            {
                value |= (uint)(b << i);
                break;
            }

            value |= (uint)((b & 0x7f) << i);
            i += 7;
            if (i > 28)
            {
                throw new OverflowException("Encoded varint is larger than uint.MaxValue");
            }
        }

        if (bytesRead == 0)
        {
            throw new InvalidOperationException("Unexpected end of span reading varint.");
        }

        return bytesRead;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ReadVarint(ReadOnlySpan<byte> source, out int value)
    {
        var bytesRead = ReadUnsignedVarint(source, out var unsignedValue);
        value = (int)((unsignedValue >> 1) ^ -(unsignedValue & 1));
        return bytesRead;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ReadGuidBigEndian(ReadOnlySpan<byte> source, out Guid value)
    {
        // Read the Guid bytes in big-endian order
        value = new Guid(
            BinaryPrimitives.ReadInt32BigEndian(source.Slice(0, 4)),
            BinaryPrimitives.ReadInt16BigEndian(source.Slice(4, 2)),
            BinaryPrimitives.ReadInt16BigEndian(source.Slice(6, 2)),
            source[8],
            source[9],
            source[10],
            source[11],
            source[12],
            source[13],
            source[14],
            source[15]
        );
        
        return 16;
    }
}