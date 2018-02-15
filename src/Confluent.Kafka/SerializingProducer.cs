// Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider
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
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Runtime.InteropServices;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Serialization;
using System.Collections.Concurrent;


namespace Confluent.Kafka
{
    internal class SerializingProducer<TKey, TValue> : ISerializingProducer<TKey, TValue>
    {
        protected readonly Producer producer;

        public ISerializer<TKey> KeySerializer { get; }

        public ISerializer<TValue> ValueSerializer { get; }

        public SerializingProducer(Producer producer, ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
        {
            this.producer = producer;
            KeySerializer = keySerializer;
            ValueSerializer = valueSerializer;

            if (keySerializer != null && keySerializer == valueSerializer)
            {
                throw new ArgumentException("Key and value serializers must not be the same object.");
            }

            if (KeySerializer == null)
            {
                if (typeof(TKey) != typeof(Null))
                {
                    throw new ArgumentNullException("Key serializer must be specified.");
                }
            }

            if (ValueSerializer == null)
            {
                if (typeof(TValue) != typeof(Null))
                {
                    throw new ArgumentNullException("Value serializer must be specified.");
                }
            }
        }

        private class TypedTaskDeliveryHandlerShim : TaskCompletionSource<Message<TKey, TValue>>, IDeliveryHandler
        {
            public TypedTaskDeliveryHandlerShim(TKey key, TValue val)
#if !NET45
                : base(TaskCreationOptions.RunContinuationsAsynchronously)
#endif
            {
                Key = key;
                Value = val;
            }

            public TKey Key;

            public TValue Value;

            public bool MarshalData { get { return false; } }

            public void HandleDeliveryReport(Message message)
            {
                var mi = new Message<TKey, TValue>(
                    message.Topic,
                    message.Partition,
                    message.Offset,
                    Key,
                    Value,
                    message.Timestamp,
                    message.Headers,
                    message.Error
                );

#if NET45
                System.Threading.Tasks.Task.Run(() => SetResult(mi));
#else
                SetResult(mi);
#endif
            }
        }

        private Task<Message<TKey, TValue>> ProduceImpl(
            string topic, Partition partition, 
            TKey key, TValue val, 
            Timestamp timestamp, 
            IEnumerable<Header> headers, 
            bool blockIfQueueFull)
        {
            var handler = new TypedTaskDeliveryHandlerShim(key, val);
            var keyBytes = KeySerializer?.Serialize(topic, key);
            var valBytes = ValueSerializer?.Serialize(topic, val);
            
            producer.ProduceImpl(
                topic, 
                valBytes, 0, valBytes == null ? 0 : valBytes.Length, 
                keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length, 
                timestamp, partition, headers, 
                blockIfQueueFull, handler);

            return handler.Task;
        }

        private class TypedDeliveryHandlerShim : IDeliveryHandler
        {
            public TypedDeliveryHandlerShim(TKey key, TValue val, IDeliveryHandler<TKey, TValue> handler)
            {
                Key = key;
                Value = val;
                Handler = handler;
            }

            public TKey Key;

            public TValue Value;

            public bool MarshalData { get { return false; } }

            public IDeliveryHandler<TKey, TValue> Handler;

            public void HandleDeliveryReport(Message message)
            {
                Handler.HandleDeliveryReport(new Message<TKey, TValue>(
                    message.Topic,
                    message.Partition,
                    message.Offset,
                    Key,
                    Value,
                    message.Timestamp,
                    message.Headers,
                    message.Error
                ));
            }
        }

        private void ProduceImpl(
            string topic, Partition partition, 
            TKey key, TValue val, 
            Timestamp timestamp, 
            IEnumerable<Header> headers, 
            bool blockIfQueueFull, 
            IDeliveryHandler<TKey, TValue> deliveryHandler)
        {
            var handler = new TypedDeliveryHandlerShim(key, val, deliveryHandler);
            
            var keyBytes = KeySerializer?.Serialize(topic, key);
            var valBytes = ValueSerializer?.Serialize(topic, val);

            producer.ProduceImpl(
                topic, 
                valBytes, 0, valBytes == null ? 0 : valBytes.Length, 
                keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length, 
                timestamp, partition, headers, blockIfQueueFull, 
                handler);
        }

#region obsolete produce methods

        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, int partition, bool blockIfQueueFull)
            => ProduceImpl(topic, partition, key, val, Timestamp.Default, null, blockIfQueueFull);

        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, int partition)
            => ProduceImpl(topic, partition, key, val, Timestamp.Default, null, producer.blockIfQueueFullPropertyValue);

        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, bool blockIfQueueFull)
            => ProduceImpl(topic, Partition.Any, key, val, Timestamp.Default, null, blockIfQueueFull);

        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => ProduceImpl(topic, Partition.Any, key, val, Timestamp.Default, null, producer.blockIfQueueFullPropertyValue, deliveryHandler);

        public void ProduceAsync(string topic, TKey key, TValue val, int partition, bool blockIfQueueFull, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => ProduceImpl(topic, partition, key, val, Timestamp.Default, null, blockIfQueueFull, deliveryHandler);

        public void ProduceAsync(string topic, TKey key, TValue val, int partition, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => ProduceImpl(topic, partition, key, val, Timestamp.Default, null, producer.blockIfQueueFullPropertyValue, deliveryHandler);

        public void ProduceAsync(string topic, TKey key, TValue val, bool blockIfQueueFull, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => ProduceImpl(topic, Partition.Any, key, val, Timestamp.Default, null, blockIfQueueFull, deliveryHandler);

#endregion

        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val)
            => ProduceImpl(topic, Partition.Any, key, val, Timestamp.Default, null, producer.blockIfQueueFullPropertyValue);

        public Task<Message<TKey, TValue>> ProduceAsync(Message<TKey, TValue> message)
            => ProduceImpl(
                message.Topic, message.Partition, 
                message.Key, message.Value, 
                message.Timestamp.Type == TimestampType.CreateTime ? message.Timestamp : Timestamp.Default, 
                message.Headers, 
                producer.blockIfQueueFullPropertyValue);

        public Task<Message<TKey, TValue>> ProduceAsync(string topic, Partition partition, TKey key, TValue val, Timestamp timestamp, IEnumerable<Header> headers)
            => ProduceImpl(topic, partition, key, val, timestamp, headers, producer.blockIfQueueFullPropertyValue);

        public void Produce(Message<TKey, TValue> message, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => ProduceImpl(
                message.Topic, message.Partition, 
                message.Key, message.Value, 
                message.Timestamp.Type == TimestampType.CreateTime ? message.Timestamp : Timestamp.Default, 
                message.Headers, 
                producer.blockIfQueueFullPropertyValue, 
                deliveryHandler);

        public void Produce(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => ProduceImpl(topic, Partition.Any, key, val, Timestamp.Default, null, producer.blockIfQueueFullPropertyValue, deliveryHandler);

        public void Produce(string topic, Partition partition, TKey key, TValue val, Timestamp timestamp, IEnumerable<Header> headers, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => ProduceImpl(topic, partition, key, val, timestamp, headers, producer.blockIfQueueFullPropertyValue, deliveryHandler);
    }
}
