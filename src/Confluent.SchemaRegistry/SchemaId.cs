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
using Confluent.Kafka;

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

    private const int DefaultInitialBufferSize = 1024;

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
    public List<int> MessageIndexes { get; set;  }

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

    public Stream FromBytes(Stream stream)
    {
        var reader = new BinaryReader(stream);
        var magicByte = reader.ReadByte();
        if (magicByte == MAGIC_BYTE_V0)
        {
            Id = IPAddress.NetworkToHostOrder(reader.ReadInt32());
        }
        else if (magicByte == MAGIC_BYTE_V1)
        {
            Guid = Utils.GuidFromBigEndian(reader.ReadBytes(16));
        }
        else
        {
            throw new InvalidDataException($"Invalid magic byte: {magicByte}");
        }

        if (SchemaType == SchemaType.Protobuf)
        {
            var size = stream.ReadVarint();
            if (size == 0)
            {
                MessageIndexes = _defaultIndex;
            }
            else
            {
                MessageIndexes = new List<int>();
                for (int i = 0; i < size; i++)
                {
                    MessageIndexes.Add(stream.ReadVarint());
                }
            }
        }
        return stream;
    }

    public byte[] IdToBytes()
    {
        if (Id == null)
        {
            throw new InvalidOperationException("Schema ID is not set.");
        }
        using (var stream = new MemoryStream(DefaultInitialBufferSize))
        using (var writer = new BinaryWriter(stream))
        {
            writer.Write(MAGIC_BYTE_V0);
            writer.Write(IPAddress.HostToNetworkOrder(Id.Value));
            WriteMessageIndexes(stream);
            return stream.ToArray();
        }
    }

    public byte[] GuidToBytes()
    {
        if (Guid == null)
        {
            throw new InvalidOperationException("Schema GUID is not set.");
        }
        using (var stream = new MemoryStream(DefaultInitialBufferSize))
        using (var writer = new BinaryWriter(stream))
        {
            writer.Write(MAGIC_BYTE_V1);
            writer.Write(Guid.Value.ToBigEndian());
            WriteMessageIndexes(stream);
            return stream.ToArray();
        }
    }

    private void WriteMessageIndexes(Stream stream)
    {
        if (MessageIndexes != null && MessageIndexes.Count > 0)
        {
            if (MessageIndexes.Count == 1 && MessageIndexes[0] == 0)
            {
                // optimization for the special case [0]
                stream.WriteVarint(0);
            }
            else
            {
                stream.WriteVarint((uint)MessageIndexes.Count);
                foreach (var index in MessageIndexes)
                {
                    stream.WriteVarint((uint)index);
                }
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
    public static ISchemaIdSerializer ToSerializer(this SchemaIdSerializerStrategy strategy)
    {
        switch (strategy)
        {
            case SchemaIdSerializerStrategy.Header:
                return new HeaderSchemaIdSerializer();
            case SchemaIdSerializerStrategy.Prefix:
                return new PrefixSchemaIdSerializer();
            default:
                throw new ArgumentException($"Unknown SchemaIdSerializerStrategy: {strategy}");
        }
    }
    public static ISchemaIdDeserializer ToDeserializer(this SchemaIdDeserializerStrategy strategy)
    {
        switch (strategy)
        {
            case SchemaIdDeserializerStrategy.Dual:
                return new DualSchemaIdDeserializer();
            case SchemaIdDeserializerStrategy.Prefix:
                return new PrefixSchemaIdDeserializer();
            default:
                throw new ArgumentException($"Unknown SchemaIdDeserializerStrategy: {strategy}");
        }
    }
}

/// <summary>
///     Interface for schema ID or GUID serialization.
/// </summary>
public interface ISchemaIdSerializer
{
    /// <summary>
    ///     Serialize a schema ID/GUID.
    ///     During serialization, the headers or payload may be mutated, and
    ///     the result is a payload (which may contain a schema ID/GUID).
    /// </summary>
    ///
    /// <param name="payload"></param>
    /// <param name="context"></param>
    /// <param name="schemaId"></param>
    /// <returns></returns>
    public byte[] Serialize(byte[] payload, SerializationContext context, SchemaId schemaId);
}

/// <summary>
///     Interface for schema ID or GUID deserialization.
/// </summary>
public interface ISchemaIdDeserializer
{
    /// <summary>
    ///     Deserialize a schema ID/GUID.
    ///     During deserialization, the schemaId is mutated, and the result is the payload
    ///     (which will not contain a schema ID/GUID).
    /// </summary>
    ///
    /// <param name="payload"></param>
    /// <param name="context"></param>
    /// <param name="schemaId"></param>
    /// <returns></returns>
    public Stream Deserialize(byte[] payload, SerializationContext context, ref SchemaId schemaId);
}

public class HeaderSchemaIdSerializer : ISchemaIdSerializer
{
    public byte[] Serialize(byte[] payload, SerializationContext context, SchemaId schemaId)
    {
        if (context.Headers == null)
        {
            throw new ArgumentNullException("Headers cannot be null");
        }
        string headerKey = context.Component == MessageComponentType.Key
            ? SchemaId.KEY_SCHEMA_ID_HEADER : SchemaId.VALUE_SCHEMA_ID_HEADER;
        context.Headers.Add(headerKey, schemaId.GuidToBytes());
        return payload;
    }
}

public class PrefixSchemaIdSerializer : ISchemaIdSerializer
{
    public byte[] Serialize(byte[] payload, SerializationContext context, SchemaId schemaId)
    {
        byte[] idBytes = schemaId.IdToBytes();
        byte[] result = new byte[idBytes.Length + payload.Length];
        Buffer.BlockCopy(idBytes, 0, result, 0, idBytes.Length);
        Buffer.BlockCopy(payload, 0, result, idBytes.Length, payload.Length);
        return result;
    }
}

public class DualSchemaIdDeserializer : ISchemaIdDeserializer
{
    public Stream Deserialize(byte[] payload, SerializationContext context, ref SchemaId schemaId)
    {
        string headerKey = context.Component == MessageComponentType.Key
            ? SchemaId.KEY_SCHEMA_ID_HEADER : SchemaId.VALUE_SCHEMA_ID_HEADER;
        if (context.Headers != null && context.Headers.TryGetLastBytes(headerKey, out var headerValue))
        {
            schemaId.FromBytes(new MemoryStream(headerValue));
            return new MemoryStream(payload);
        }

        return schemaId.FromBytes(new MemoryStream(payload));
    }
}

public class PrefixSchemaIdDeserializer : ISchemaIdDeserializer
{
    public Stream Deserialize(byte[] payload, SerializationContext context, ref SchemaId schemaId)
    {
        return schemaId.FromBytes(new MemoryStream(payload));
    }
}