﻿// Copyright 2016-2019 Confluent Inc.
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

using System.Threading.Tasks;
using System.Collections.Generic;
using Avro.Generic;
using Confluent.Kafka;
using System;
using System.Linq;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     (async) Avro deserializer. Use this deserializer with GenericRecord,
    ///     types generated using the avrogen.exe tool or one of the following 
    ///     primitive types: int, long, float, double, boolean, string, byte[].
    /// </summary>
    /// <remarks>
    ///     Serialization format:
    ///       byte 0:           Magic byte use to identify the protocol format.
    ///       bytes 1-4:        Unique global id of the Avro schema that was used for encoding (as registered in Confluent Schema Registry), big endian.
    ///       following bytes:  The serialized data.
    /// </remarks>
    public class AvroDeserializer<T> : IAsyncDeserializer<T>
    {
        private ISchemaRegistryClient schemaRegistryClient;
        private AvroDeserializerConfig config;
        private IList<IRuleExecutor> ruleExecutors;

        private IAsyncDeserializer<T> deserializerImpl;

        public AvroDeserializer(ISchemaRegistryClient schemaRegistryClient)
            : this(schemaRegistryClient, null)
        {
        }

        /// <summary>
        ///     Initialize a new AvroDeserializer instance.
        /// </summary>
        /// <param name="schemaRegistryClient">
        ///     An implementation of ISchemaRegistryClient used for
        ///     communication with Confluent Schema Registry.
        /// </param>
        /// <param name="config">
        ///     Deserializer configuration properties (refer to 
        ///     <see cref="AvroDeserializerConfig" />).
        /// </param>
        public AvroDeserializer(ISchemaRegistryClient schemaRegistryClient, IEnumerable<KeyValuePair<string, string>> config = null) 
            : this(schemaRegistryClient, config != null ? new AvroDeserializerConfig(config) : null)
        {
        }

        public AvroDeserializer(ISchemaRegistryClient schemaRegistryClient, AvroDeserializerConfig config = null, IList<IRuleExecutor> ruleExecutors = null)
        {
            this.schemaRegistryClient = schemaRegistryClient;
            this.config = config;
            this.ruleExecutors = ruleExecutors ?? new List<IRuleExecutor>();
            
            if (config == null) { return; }

            var nonAvroConfig = config
                .Where(item => !item.Key.StartsWith("avro.") && !item.Key.StartsWith("rules."));
            if (nonAvroConfig.Count() > 0)
            {
                throw new ArgumentException($"AvroDeserializer: unknown configuration parameter {nonAvroConfig.First().Key}.");
            }

            var avroConfig = config
                .Where(item => item.Key.StartsWith("avro.") && !item.Key.StartsWith("rules."));
            foreach (var property in avroConfig)
            {
                if (property.Key != AvroDeserializerConfig.PropertyNames.UseLatestVersion &&
                    property.Key != AvroDeserializerConfig.PropertyNames.UseLatestWithMetadata &&
                    property.Key != AvroSerializerConfig.PropertyNames.SubjectNameStrategy)
                {
                    throw new ArgumentException($"AvroDeserializer: unknown configuration parameter {property.Key}");
                }
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
        public async Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            try
            {
                if (isNull)
                {
                    // Jit optimizes this check away
                    if (default(T) == null)
                    {
                        return default;
                    }
                    else
                    {
                        throw new InvalidOperationException("Cannot deserialize null to a Value Type");
                    }
                }

                if (deserializerImpl == null)
                {
                    deserializerImpl = (typeof(T) == typeof(GenericRecord))
                        ? (IAsyncDeserializer<T>)new GenericDeserializerImpl(schemaRegistryClient, config, ruleExecutors)
                        : new SpecificDeserializerImpl<T>(schemaRegistryClient, config, ruleExecutors);
                }

                return isNull ? default : await deserializerImpl.DeserializeAsync(data, isNull, context)
                    .ConfigureAwait(continueOnCapturedContext: false);
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }
    }
}
