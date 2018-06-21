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
    internal class SerializingProducer<TKey, TValue>
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

        private class TypedTaskDeliveryHandlerShim : TaskCompletionSource<DeliveryReport<TKey, TValue>>, IDeliveryHandler
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

            public bool MarshalHeaders { get { return true; } }

            public void HandleDeliveryReport(DeliveryReport deliveryReport)
            {
                var mi = new DeliveryReport<TKey, TValue>
                {
                    TopicPartitionOffsetError = deliveryReport.TopicPartitionOffsetError,
                    Message = new Message<TKey, TValue>{ Key = Key, Value = Value, Timestamp = deliveryReport.Message.Timestamp, Headers = deliveryReport.Message.Headers }
                };

#if NET45
                System.Threading.Tasks.Task.Run(() => SetResult(mi));
#else
                SetResult(mi);
#endif
            }
        }

        private Task<DeliveryReport<TKey, TValue>> ProduceImpl(
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
            public TypedDeliveryHandlerShim(TKey key, TValue val, bool marshalHeaders, IDeliveryHandler<TKey, TValue> handler)
            {
                Key = key;
                Value = val;
                Handler = handler;
                MarshalHeaders = marshalHeaders;
            }

            public TKey Key;

            public TValue Value;

            public bool MarshalData { get { return false; } }

            public bool MarshalHeaders { get; private set; }

            public IDeliveryHandler<TKey, TValue> Handler;

            public void HandleDeliveryReport(DeliveryReport deliveryReport)
            {
                Handler.HandleDeliveryReport(
                    new DeliveryReport<TKey, TValue>
                    {
                        TopicPartitionOffsetError = deliveryReport.TopicPartitionOffsetError,
                        Message = new Message<TKey, TValue> { Key = Key, Value = Value, Timestamp = deliveryReport.Message.Timestamp, Headers = deliveryReport.Message.Headers }
                    }
                );
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
            var handler = new TypedDeliveryHandlerShim(key, val, producer.enableDeliveryReportHeaderMarshaling, deliveryHandler);
            
            var keyBytes = KeySerializer?.Serialize(topic, key);
            var valBytes = ValueSerializer?.Serialize(topic, val);

            producer.ProduceImpl(
                topic, 
                valBytes, 0, valBytes == null ? 0 : valBytes.Length, 
                keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length, 
                timestamp, partition, headers, blockIfQueueFull, 
                handler);
        }

        private class TypedDeliveryHandlerShim_Action : IDeliveryHandler
        {
            public TypedDeliveryHandlerShim_Action(TKey key, TValue val, bool marshalHeaders, Action<DeliveryReport<TKey, TValue>> handler)
            {
                Key = key;
                Value = val;
                Handler = handler;
                MarshalHeaders = marshalHeaders;
            }

            public TKey Key;

            public TValue Value;

            public bool MarshalData { get { return false; } }

            public bool MarshalHeaders { get; private set; }

            public Action<DeliveryReport<TKey, TValue>> Handler;

            public void HandleDeliveryReport(DeliveryReport deliveryReport)
            {
                Handler(
                    new DeliveryReport<TKey, TValue>
                    {
                        TopicPartitionOffsetError = deliveryReport.TopicPartitionOffsetError,
                        Message = new Message<TKey, TValue> { Key = Key, Value = Value, Timestamp = deliveryReport.Message.Timestamp, Headers = deliveryReport.Message.Headers }
                    }
                );
            }
        }

        private void ProduceImpl(
            string topic, Partition partition, 
            TKey key, TValue val, 
            Timestamp timestamp, 
            IEnumerable<Header> headers, 
            bool blockIfQueueFull, 
            Action<DeliveryReport<TKey, TValue>> deliveryHandler)
        {
            var handler = new TypedDeliveryHandlerShim_Action(key, val, producer.enableDeliveryReportHeaderMarshaling, deliveryHandler);
            
            var keyBytes = KeySerializer?.Serialize(topic, key);
            var valBytes = ValueSerializer?.Serialize(topic, val);

            producer.ProduceImpl(
                topic, 
                valBytes, 0, valBytes == null ? 0 : valBytes.Length, 
                keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length, 
                timestamp, partition, headers, blockIfQueueFull, 
                handler);
        }

        public Task<DeliveryReport<TKey, TValue>> ProduceAsync(string topic, Message<TKey, TValue> message)
            => ProduceImpl(
                topic, Partition.Any,
                message.Key, message.Value,
                message.Timestamp, message.Headers,
                producer.blockIfQueueFullPropertyValue);

        public Task<DeliveryReport<TKey, TValue>> ProduceAsync(TopicPartition topicPartition, Message<TKey, TValue> message)
            => ProduceImpl(
                topicPartition.Topic, topicPartition.Partition,
                message.Key, message.Value,
                message.Timestamp, message.Headers,
                producer.blockIfQueueFullPropertyValue);

        public void Produce(TopicPartition topicPartition, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler)
            => ProduceImpl(
                topicPartition.Topic, topicPartition.Partition,
                 message.Key, message.Value, 
                 message.Timestamp, message.Headers,
                 producer.blockIfQueueFullPropertyValue, deliveryHandler);

        public void Produce(string topic, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler)
            => ProduceImpl(
                topic, Partition.Any, 
                message.Key, message.Value, 
                message.Timestamp, message.Headers, 
                producer.blockIfQueueFullPropertyValue, deliveryHandler);
    }
}
