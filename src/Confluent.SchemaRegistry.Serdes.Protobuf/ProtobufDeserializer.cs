// Copyright 2020 Confluent Inc.
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
using System.Linq;
using System.Threading.Tasks;
using System.Net;
using Confluent.Kafka;
using Google.Protobuf;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     (async) Protobuf deserializer.
    /// </summary>
    /// <remarks>
    ///     Serialization format:
    ///       byte 0:           A magic byte that identifies this as a message with
    ///                         Confluent Platform framing.
    ///       bytes 1-4:        Unique global id of the Protobuf schema that was used
    ///                         for encoding (as registered in Confluent Schema Registry),
    ///                         big endian.
    ///       following bytes:  1. A size-prefixed array of indices that identify the
    ///                            specific message type in the schema (a given schema
    ///                            can contain many message types and they can be nested).
    ///                            Size and indices are unsigned varints. The common case
    ///                            where the message type is the first message in the
    ///                            schema (i.e. index data would be [1,0]) is encoded as
    ///                            a single 0 byte as an optimization.
    ///                         2. The protobuf serialized data.
    /// </remarks>
    public class ProtobufDeserializer<T> : IAsyncDeserializer<T> where T : class, IMessage<T>, new()
    {
        private bool useDeprecatedFormat = false;
        
        private MessageParser<T> parser;

        /// <summary>
        ///     Initialize a new ProtobufDeserializer instance.
        /// </summary>
        /// <param name="config">
        ///     Deserializer configuration properties (refer to 
        ///     <see cref="ProtobufDeserializerConfig" />).
        /// </param>
        public ProtobufDeserializer(IEnumerable<KeyValuePair<string, string>> config = null)
        {
            this.parser = new MessageParser<T>(() => new T());

            if (config == null) { return; }

            var nonProtobufConfig = config.Where(item => !item.Key.StartsWith("protobuf."));
            if (nonProtobufConfig.Count() > 0)
            {
                throw new ArgumentException($"ProtobufDeserializer: unknown configuration parameter {nonProtobufConfig.First().Key}");
            }

            ProtobufDeserializerConfig protobufConfig = new ProtobufDeserializerConfig(config);
            if (protobufConfig.UseDeprecatedFormat != null)
            {
                this.useDeprecatedFormat = protobufConfig.UseDeprecatedFormat.Value;
            }
        }

        /// <summary>
        ///     Deserialize an object of type <typeparamref name="T"/>
        ///     from a byte array.
        /// </summary>
        /// <param name="data">
        ///     The raw byte data to deserialize.
        /// </param>
        /// <param name="isNull">
        ///     True if this is a null value.
        /// </param>
        /// <param name="context">
        ///     Context relevant to the deserialize operation.
        /// </param>
        /// <returns>
        ///     A <see cref="System.Threading.Tasks.Task" /> that completes
        ///     with the deserialized value.
        /// </returns>
        public Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull) { return Task.FromResult<T>(null); }

            var array = data.ToArray();
            if (array.Length < 6)
            {
                throw new InvalidDataException($"Expecting data framing of length 6 bytes or more but total data size is {array.Length} bytes");
            }

            try
            {
                using (var stream = new MemoryStream(array))
                using (var reader = new BinaryReader(stream))
                {
                    var magicByte = reader.ReadByte();
                    if (magicByte != Constants.MagicByte)
                    {
                        throw new InvalidDataException($"Expecting message {context.Component.ToString()} with Confluent Schema Registry framing. Magic byte was {array[0]}, expecting {Constants.MagicByte}");
                    }

                    // A schema is not required to deserialize protobuf messages since the
                    // serialized data includes tag and type information, which is enough for
                    // the IMessage<T> implementation to deserialize the data (even if the
                    // schema has evolved). _schemaId is thus unused.
                    var _schemaId = IPAddress.NetworkToHostOrder(reader.ReadInt32());

                    // Read the index array length, then all of the indices. These are not
                    // needed, but parsing them is the easiest way to seek to the start of
                    // the serialized data because they are varints.
                    var indicesLength = useDeprecatedFormat ? (int)stream.ReadUnsignedVarint() : stream.ReadVarint();
                    for (int i=0; i<indicesLength; ++i)
                    {
                        if (useDeprecatedFormat)
                        {
                            stream.ReadUnsignedVarint();
                        }
                        else
                        {
                            stream.ReadVarint();
                        }
                    }
                    return Task.FromResult(parser.ParseFrom(stream));
                }
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }
    }
}
