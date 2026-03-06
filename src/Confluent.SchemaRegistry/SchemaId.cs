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
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Confluent.Shared;

namespace Confluent.SchemaRegistry;

/// <summary>
///     Represents a schema ID or GUID.
/// </summary>
public struct SchemaId
{
    public const string KEY_SCHEMA_ID_HEADER = "__key_schema_id";
    public const string VALUE_SCHEMA_ID_HEADER = "__value_schema_id";
    public const byte MAGIC_BYTE_V0 = 0;
    public const byte MAGIC_BYTE_V1 = 1;

    private static List<int> _defaultIndex = new List<int> { 0 };

    /// <summary>
    ///     The schema type.
    /// </summary>
    public SchemaType SchemaType { get; }

    /// <summary>
    ///     The schema ID.
    /// </summary>
    public int? Id { get; set;  }

    /// <summary>
    ///     The schema GUID.
    /// </summary>
    public Guid? Guid { get; set;  }

    /// <summary>
    ///     The schema GUID.
    /// </summary>
    public IReadOnlyList<int> MessageIndexes { get; set;  }

    public SchemaId(SchemaType schemaType)
    {
        SchemaType = schemaType;
        Id = null;
        Guid = null;
        MessageIndexes = null;
    }

    public SchemaId(SchemaType schemaType, int id, Guid guid)
    {
        SchemaType = schemaType;
        Id = id;
        Guid = guid;
        MessageIndexes = null;
    }

    public SchemaId(SchemaType schemaType, int id, string guid)
    {
        SchemaType = schemaType;
        Id = id;
        Guid = guid != null ? System.Guid.Parse(guid) : null;
        MessageIndexes = null;
    }
    
    public SchemaId(SchemaType schemaType, int id, string guid, IReadOnlyList<int> messageIndexes)
    {
        SchemaType = schemaType;
        Id = id;
        Guid = guid != null ? System.Guid.Parse(guid) : null;
        MessageIndexes = messageIndexes;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlyMemory<byte> FromBytes(ReadOnlyMemory<byte> payload)
    {
        var offset = BinaryConverter.ReadByte(payload.Span, out var magicByte);
        
        if (magicByte == MAGIC_BYTE_V0)
        {
            offset += BinaryConverter.ReadInt32(payload.Slice(offset).Span, out var id);
            Id = id;
        }
        else if (magicByte == MAGIC_BYTE_V1)
        {
            offset += BinaryConverter.ReadGuidBigEndian(payload.Slice(offset).Span, out var guid);
            Guid = guid;
        }
        else
        {
            throw new InvalidDataException($"Invalid magic byte: {magicByte}");
        }
        
        if (SchemaType == SchemaType.Protobuf)
        {
            offset += BinaryConverter.ReadVarint(payload.Slice(offset).Span, out var size);
            if (size == 0)
            {
                MessageIndexes = _defaultIndex;
            }
            else
            {
                var messageIndexes = new List<int>();
                for (int i = 0; i < size; i++)
                {
                    offset += BinaryConverter.ReadVarint(payload.Slice(offset).Span, out var index);
                    messageIndexes.Add(index);
                }

                MessageIndexes = messageIndexes;
            }
        }

        return payload.Slice(offset);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int CalculateIdSize()
    {
        return sizeof(byte) // Magic byte
               + sizeof(int) // Schema ID
               + CalculateMessageIndexesSize();
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteIdToBytes(Span<byte> destination)
    {
        if (Id == null)
        {
            throw new InvalidOperationException("Schema ID is not set.");
        }

        var offset = BinaryConverter.WriteByte(destination, MAGIC_BYTE_V0);
        offset += BinaryConverter.WriteInt32(destination.Slice(offset), Id.Value);
        WriteArrayIndexesToBytes(destination.Slice(offset));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int CalculateGuidSize()
    {
        return sizeof(byte) // Magic byte
               + 16 // GUID size
               + CalculateMessageIndexesSize();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteGuidToBytes(Span<byte> destination)
    {
        if (Guid == null)
        {
            throw new InvalidOperationException("Schema GUID is not set.");
        }

        var offset = BinaryConverter.WriteByte(destination, MAGIC_BYTE_V1);
        offset += BinaryConverter.WriteGuidBigEndian(destination.Slice(offset), Guid.Value);
        WriteArrayIndexesToBytes(destination.Slice(offset));
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int CalculateMessageIndexesSize()
    {
        if (MessageIndexes == null || MessageIndexes.Count == 0)
        {
            return 0; // No message indexes to serialize
        }

        int size = 0;
        if (MessageIndexes.Count == 1 && MessageIndexes[0] == 0)
        {
            // Optimization for the common case where the message type is the first in the schema
            size += 1;
        }
        else
        {
            Span<byte> scratchBuffer = stackalloc byte[sizeof(uint)];
            size += BinaryConverter.WriteVarint(scratchBuffer, (uint)MessageIndexes.Count);
            foreach (var index in MessageIndexes)
            {
                size += BinaryConverter.WriteVarint(scratchBuffer, (uint)index);
            }
        }

        return size;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void WriteArrayIndexesToBytes(Span<byte> destination)
    {
        if (MessageIndexes == null || MessageIndexes.Count == 0)
        {
            // No message indexes to serialize
            return;
        }
        
        int offset = 0;
        if (MessageIndexes.Count == 1 && MessageIndexes[0] == 0)
        {
            // Optimization for the common case where the message type is the first in the schema
            BinaryConverter.WriteVarint(destination.Slice(offset), 0);
        }
        else
        {
            offset += BinaryConverter.WriteVarint(destination.Slice(offset), (uint)MessageIndexes.Count);
            foreach (var index in MessageIndexes)
            {
                offset += BinaryConverter.WriteVarint(destination.Slice(offset), (uint)index);
            }
        }
    }    
}

/// <summary>
///    The schema ID serialization strategy.
/// </summary>
public enum SchemaIdSerializerStrategy
{
    Header,
    Prefix
}

/// <summary>
///     The schema ID deserialization strategy.
/// </summary>
public enum SchemaIdDeserializerStrategy
{
    Dual,
    Prefix
}

public static class SchemaIdStrategyExtensions
{
    public static ISchemaIdEncoder ToEncoder(this SchemaIdSerializerStrategy strategy)
    {
        switch (strategy)
        {
            case SchemaIdSerializerStrategy.Header:
                return new HeaderSchemaIdEncoder();
            case SchemaIdSerializerStrategy.Prefix:
                return new PrefixSchemaIdEncoder();
            default:
                throw new ArgumentException($"Unknown SchemaIdSerializerStrategy: {strategy}");
        }
    }
    public static ISchemaIdDecoder ToDeserializer(this SchemaIdDeserializerStrategy strategy)
    {
        switch (strategy)
        {
            case SchemaIdDeserializerStrategy.Dual:
                return new DualSchemaIdDecoder();
            case SchemaIdDeserializerStrategy.Prefix:
                return new PrefixSchemaIdDecoder();
            default:
                throw new ArgumentException($"Unknown SchemaIdDeserializerStrategy: {strategy}");
        }
    }
}

/// <summary>
/// Interface for encoding schema IDs or GUIDs.
/// </summary>
public interface ISchemaIdEncoder
{
    /// <summary>
    /// Encodes the schema ID or GUID into the provided buffer. The buffer is expected to be large enough 
    /// to hold the data. Use <see cref="CalculateSize"/> to determine the required size.
    /// </summary>
    void Encode(Span<byte> buffer, ref SerializationContext context, ref SchemaId schemaId);

    /// <summary>
    /// Calculates the number of bytes required to encode the schema ID or GUID. 
    /// Returns 0 if the schema is encoded in headers instead of the buffer.
    /// </summary>
    int CalculateSize(ref SchemaId schemaId);
}

internal class PrefixSchemaIdEncoder : ISchemaIdEncoder
{
    public void Encode(Span<byte> buffer, ref SerializationContext context, ref SchemaId schemaId)
    {
        schemaId.WriteIdToBytes(buffer);
    }

    public int CalculateSize(ref SchemaId schemaId)
    {
        return schemaId.CalculateIdSize();
    }
}

internal class HeaderSchemaIdEncoder : ISchemaIdEncoder
{
    public void Encode(Span<byte> buffer, ref SerializationContext context, ref SchemaId schemaId)
    {
        string headerKey = context.Component == MessageComponentType.Key
            ? SchemaId.KEY_SCHEMA_ID_HEADER : SchemaId.VALUE_SCHEMA_ID_HEADER;
        
        var bytes = new byte[schemaId.CalculateGuidSize()];
        schemaId.WriteGuidToBytes(bytes);
        context.Headers.Add(headerKey, bytes);
    }

    public int CalculateSize(ref SchemaId schemaId)
    {
        return 0;
    }
}

/// <summary>
/// Interface for decoding schema IDs or GUIDs.
/// </summary>
public interface ISchemaIdDecoder
{
    /// <summary>
    /// Decodes a schema ID or GUID from the provided payload.
    /// </summary>
    /// <param name="payload">The payload that may contain an encoded schema ID or GUID.</param>
    /// <param name="context">The serialization context, including headers and the message component type.</param>
    /// <param name="schemaId">The <see cref="SchemaId"/> instance to populate with the decoded value.</param>
    /// <returns>The remaining payload after the schema ID or GUID has been decoded.</returns>
    ReadOnlyMemory<byte> Decode(ReadOnlyMemory<byte> payload, SerializationContext context, ref SchemaId schemaId);
}

internal class PrefixSchemaIdDecoder : ISchemaIdDecoder
{
    public ReadOnlyMemory<byte> Decode(ReadOnlyMemory<byte> payload, SerializationContext context, ref SchemaId schemaId)
    {
        return schemaId.FromBytes(payload);
    }
}

internal class DualSchemaIdDecoder : ISchemaIdDecoder
{
    public ReadOnlyMemory<byte> Decode(ReadOnlyMemory<byte> payload, SerializationContext context,
        ref SchemaId schemaId)
    {
        var headerKey = context.Component == MessageComponentType.Key
            ? SchemaId.KEY_SCHEMA_ID_HEADER 
            : SchemaId.VALUE_SCHEMA_ID_HEADER;
        
        if (context.Headers != null && context.Headers.TryGetLastBytes(headerKey, out var headerValue))
        {
            schemaId.FromBytes(headerValue);
            return payload;
        }
        return schemaId.FromBytes(payload);
    }
}