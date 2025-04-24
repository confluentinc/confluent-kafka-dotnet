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

namespace Confluent.SchemaRegistry.Serdes.Protobuf;

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
    public static int WriteBytes(Span<byte> destination, byte[] value)
    {
        value.CopyTo(destination);
        return value.Length;
    }
}