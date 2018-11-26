// Copyright 2018 Confluent Inc.
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
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Confluent.SchemaRegistry;
using Avro.Generic;


namespace Confluent.Kafka.AvroSerdes
{
    /// <summary>
    ///     Avro deserialization related extension methods for
    ///     <see cref="Confluent.Kafka.Consumer" />
    /// </summary>
    public class AvroConsumer : Consumer
    {
        private ISchemaRegistryClient schemaRegistryClient;
        
        private Dictionary<Type, object> avroDeserializers 
            = new Dictionary<Type, object>();


        /// <summary>
        ///     Creates a new AvroConsumer instance.
        /// </summary>
        /// <param name="schemaRegistryClient">
        ///     An implementation of ISchemaRegistryClient used for
        ///     communication with Confluent Schema Registry.
        /// </param>
        /// <param name="config">
        ///     A collection of librdkafka configuration properties.
        /// </param>
        public AvroConsumer(ISchemaRegistryClient schemaRegistryClient, IEnumerable<KeyValuePair<string, string>> config)
            : base(config)
        {
            this.schemaRegistryClient = schemaRegistryClient;
        }


        /// <summary>
        ///     Sets the Avro deserializer that will be used to deserialize keys or values with
        ///     the specified type.
        /// </summary>
        /// <param name="deserializer">
        ///     The Avro deserializer.
        /// </param>
        public void RegisterAvroDeserializer<T>(AvroDeserializer<T> deserializer)
        {
            avroDeserializers.Add(typeof(T), deserializer);
        }


        /// <summary>
        ///     Removes the Avro deserializer associated with the specified type.
        /// </summary>
        public void UnregisterAvroSerializer<T>()
        {
            avroDeserializers.Remove(typeof(T));
        }


        /// <summary>
        ///     Gets the Avro deserializer that will be used to deserialize values of the specified type.
        /// </summary>
        /// <returns>
        ///     The Avro deserializer corresponding to the specified type.
        /// </returns>
        public AvroDeserializer<T> GetAvroDeserializer<T>()
        {
            return (AvroDeserializer<T>)avroDeserializers[typeof(T)];
        }


        private AvroDeserializer<T> GetOrCreateAvroDeserializer<T>()
        {
            if (!avroDeserializers.ContainsKey(typeof(T)))
            {
                avroDeserializers.Add(typeof(T), new AvroDeserializer<T>());
            }

            return (AvroDeserializer<T>)avroDeserializers[typeof(T)];
        }


        /// <summary>
        ///     Poll for new messages / events. Blocks until a 
        ///     <see cref="Confluent.Kafka.ConsumeResult{TKey, TValue}" />
        ///     is available or the operation has been cancelled.
        /// </summary>
        /// <param name="keySerdeType">
        ///     The deserializer type to use to deserialize keys.
        /// </param>
        /// <param name="valueSerdeType">
        ///     The deserializer type to use to deserialize values.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used to cancel this operation.
        /// </param>
        /// <returns>
        ///     The <see cref="Confluent.Kafka.ConsumeResult{TKey, TValue}" />.
        /// </returns>
        public ConsumeResult<TKey, TValue> Consume<TKey, TValue>(
            SerdeType keySerdeType,
            SerdeType valueSerdeType,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            try
            {
                var result = Consume(cancellationToken);
                if (result == null) { return null; }

                return new ConsumeResult<TKey, TValue>
                {
                    TopicPartitionOffset = result.TopicPartitionOffset,
                    Message = new Message<TKey, TValue>
                    {
                        Timestamp = result.Timestamp,
                        Headers = result.Headers,
                        Key = keySerdeType == SerdeType.Avro
                            ? (GetOrCreateAvroDeserializer<TKey>())
                                .Deserialize(schemaRegistryClient, result.Topic, result.Key, true)
                                .ConfigureAwait(false).GetAwaiter().GetResult()
                            : GetDeserializer<TKey>()(result.Key, result.Key == null),
                        Value = valueSerdeType == SerdeType.Avro
                            ? (GetOrCreateAvroDeserializer<TValue>())
                                .Deserialize(schemaRegistryClient, result.Topic, result.Value, false)
                                .ConfigureAwait(false).GetAwaiter().GetResult()
                            : GetDeserializer<TValue>()(result.Value, result.Value == null)
                    }
                };
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }


        /// <summary>
        ///     Poll for new messages / events. Blocks until a 
        ///     <see cref="Confluent.Kafka.ConsumeResult{TKey, TValue}" />
        ///     is available or the timeout period has elapsed.
        /// </summary>
        /// <param name="keySerdeType">
        ///     The deserializer type to use to deserialize keys.
        /// </param>
        /// <param name="valueSerdeType">
        ///     The deserializer type to use to deserialize values.
        /// </param>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
        /// </param>
        /// <returns>
        ///     The <see cref="Confluent.Kafka.ConsumeResult{TKey, TValue}" />.
        /// </returns>
        public async Task<ConsumeResult<TKey, TValue>> ConsumeAsync<TKey, TValue>(
            SerdeType keySerdeType,
            SerdeType valueSerdeType,
            TimeSpan timeout)
        {
            try
            {
                var result = Consume(timeout);
                if (result == null) { return null; }

                return new ConsumeResult<TKey, TValue>
                {
                    TopicPartitionOffset = result.TopicPartitionOffset,
                    Message = new Message<TKey, TValue>
                    {
                        Timestamp = result.Timestamp,
                        Headers = result.Headers,
                        Key = keySerdeType == SerdeType.Avro
                            ? GetOrCreateAvroDeserializer<TKey>()
                                .Deserialize(schemaRegistryClient, result.Topic, result.Key, true)
                                .ConfigureAwait(false).GetAwaiter().GetResult()
                            : GetDeserializer<TKey>()(result.Key, result.Key == null),
                        Value = valueSerdeType == SerdeType.Avro
                            ? GetOrCreateAvroDeserializer<TValue>()
                                .Deserialize(schemaRegistryClient, result.Topic, result.Value, false)
                                .ConfigureAwait(false).GetAwaiter().GetResult()
                            : GetDeserializer<TValue>()(result.Value, result.Value == null)
                    }
                };
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

    }
}
