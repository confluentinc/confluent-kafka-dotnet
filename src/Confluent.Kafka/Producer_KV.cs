// Copyright 2016-2018 Confluent Inc., 2015-2016 Andreas Heider
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
    /// <summary>
    ///     Implements a high-level Apache Kafka producer with key
    ///     and value serialization.
    /// </summary>
    public class Producer<TKey, TValue> : IProducer<TKey, TValue>
    {
        private readonly Producer ownedClient;
        private readonly Handle handle;
        private Producer producer;
        private ISerializer<TKey> keySerializer;
        private ISerializer<TValue> valueSerializer;

        private void setAndValidateSerializers(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
        {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;

            if (keySerializer != null && keySerializer == valueSerializer)
            {
                throw new ArgumentException("Key and value serializers must not be the same object.");
            }

            if (keySerializer == null)
            {
                if (typeof(TKey) != typeof(Null))
                {
                    throw new ArgumentNullException("Key serializer must be specified.");
                }
            }

            if (valueSerializer == null)
            {
                if (typeof(TValue) != typeof(Null))
                {
                    throw new ArgumentNullException("Value serializer must be specified.");
                }
            }
        }

        /// <summary>
        ///     Creates a new Producer instance.
        /// </summary>
        /// <param name="config">
        ///     librdkafka configuration parameters (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
        /// </param>
        /// <param name="keySerializer">
        ///     An ISerializer implementation instance that will be used to serialize keys.
        /// </param>
        /// <param name="valueSerializer">
        ///     An ISerializer implementation instance that will be used to serialize values.
        /// </param>
        public Producer(
            IEnumerable<KeyValuePair<string, object>> config,
            ISerializer<TKey> keySerializer,
            ISerializer<TValue> valueSerializer)
        {
            var configWithoutKeySerializerProperties = keySerializer?.Configure(config, true) ?? config;
            var configWithoutValueSerializerProperties = valueSerializer?.Configure(config, false) ?? config;

            var configWithoutSerializerProperties = config.Where(item => 
                configWithoutKeySerializerProperties.Any(ci => ci.Key == item.Key) &&
                configWithoutValueSerializerProperties.Any(ci => ci.Key == item.Key)
            );

            this.ownedClient = new Producer(configWithoutSerializerProperties);
            this.handle = ownedClient.Handle;
            this.producer = ownedClient;
            setAndValidateSerializers(keySerializer, valueSerializer);
        }

        /// <summary>
        ///     Creates a new Producer instance
        /// </summary>
        /// <param name="handle">
        ///     A librdkafka handle to use for Kafka cluster communications.
        /// </param>
        /// <param name="keySerializer">
        ///     An ISerializer implementation instance that will be used to serialize keys.
        /// </param>
        /// <param name="valueSerializer">
        ///     An ISerializer implementation instance that will be used to serialize values.
        /// </param>
        public Producer(
            Handle handle,
            ISerializer<TKey> keySerializer,
            ISerializer<TValue> valueSerializer)
        {
            if (!(handle.Owner is Producer))
            {
                throw new ArgumentException("Handle must be owned by another Producer instance");
            }

            this.ownedClient = null;
            this.handle = handle;
            this.producer = (Producer)handle.Owner;
            setAndValidateSerializers(keySerializer, valueSerializer);
        }

        /// <include file='include_docs_client.xml' path='API/Member[@name="Name"]/*' />
        public string Name
            => this.handle.Owner.Name;

        /// <include file='include_docs_client.xml' path='API/Member[@name="OnLog"]/*' />
        public event EventHandler<LogMessage> OnLog
        {
            add { this.handle.Owner.OnLog += value; }
            remove { this.handle.Owner.OnLog -= value; }
        }

        /// <include file='include_docs_client.xml' path='API/Member[@name="OnStatistics"]/*' />
        public event EventHandler<string> OnStatistics
        {
            add { this.handle.Owner.OnStatistics += value; }
            remove { this.handle.Owner.OnStatistics -= value; }
        }

        /// <include file='include_docs_producer.xml' path='API/Member[@name="OnError"]/*' />
        public event EventHandler<Error> OnError
        {
            add { this.handle.Owner.OnError += value; }
            remove { this.handle.Owner.OnError -= value; }
        }

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Flush_int"]/*' />
        public int Flush(int millisecondsTimeout)
            => ((Producer)this.handle.Owner).Flush(millisecondsTimeout);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Flush_TimeSpan"]/*' />
        public int Flush(TimeSpan timeout)
            => ((Producer)this.handle.Owner).Flush(timeout.TotalMillisecondsAsInt());

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Poll_int"]/*' />
        public int Poll(int millisecondsTimeout)
            => ((Producer)this.handle.Owner).Poll(millisecondsTimeout);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Poll_TimeSpan"]/*' />
        public int Poll(TimeSpan timeout)
            => ((Producer)this.handle.Owner).Poll(timeout.TotalMillisecondsAsInt());

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Dispose"]/*' />
        public void Dispose()
        {
            if (keySerializer != null)
            {
                keySerializer.Dispose();
            }

            if (valueSerializer != null)
            {
                valueSerializer.Dispose();
            }

            if (ownedClient == this.handle.Owner) 
            {
                ownedClient.Dispose();
            }
        }

        /// <include file='include_docs_client.xml' path='API/Member[@name="AddBrokers_string"]/*' />
        public int AddBrokers(string brokers)
            => this.handle.Owner.AddBrokers(brokers);

        /// <summary>
        ///     An opaque reference to the underlying librdkafka client instance.
        /// </summary>
        public Handle Handle 
            => handle;


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


        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_Message"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Common"]/*' />
        public Task<DeliveryReport<TKey, TValue>> ProduceAsync(string topic, Message<TKey, TValue> message)
        {
            var handler = new TypedTaskDeliveryHandlerShim(message.Key, message.Value);
            var keyBytes = keySerializer?.Serialize(topic, message.Key);
            var valBytes = valueSerializer?.Serialize(topic, message.Value);
            
            producer.ProduceImpl(
                topic, 
                valBytes, 0, valBytes == null ? 0 : valBytes.Length, 
                keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length, 
                message.Timestamp, Partition.Any, message.Headers, 
                producer.blockIfQueueFullPropertyValue, handler);

            return handler.Task;
        }

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_TopicPartition_Message"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Common"]/*' />
        public Task<DeliveryReport<TKey, TValue>> ProduceAsync(TopicPartition topicPartition, Message<TKey, TValue> message)
        {
            var handler = new TypedTaskDeliveryHandlerShim(message.Key, message.Value);
            var keyBytes = keySerializer?.Serialize(topicPartition.Topic, message.Key);
            var valBytes = valueSerializer?.Serialize(topicPartition.Topic, message.Value);
            
            producer.ProduceImpl(
                topicPartition.Topic, 
                valBytes, 0, valBytes == null ? 0 : valBytes.Length, 
                keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length, 
                message.Timestamp, topicPartition.Partition, message.Headers, 
                producer.blockIfQueueFullPropertyValue, handler);

            return handler.Task;
        }

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_Message"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="Produce_Action"]/*' />
        public void Produce(TopicPartition topicPartition, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler)
        {
            var keyBytes = keySerializer?.Serialize(topicPartition.Topic, message.Key);
            var valBytes = valueSerializer?.Serialize(topicPartition.Topic, message.Value);

            producer.ProduceImpl(
                topicPartition.Topic, 
                valBytes, 0, valBytes == null ? 0 : valBytes.Length, 
                keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length, 
                message.Timestamp, topicPartition.Partition, 
                message.Headers, producer.blockIfQueueFullPropertyValue, 
                new TypedDeliveryHandlerShim_Action(message.Key, message.Value, producer.enableDeliveryReportHeaderMarshaling, deliveryHandler)
            );
        }

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_TopicPartition_Message"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="Produce_Action"]/*' />
        public void Produce(string topic, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler)
        {
            var keyBytes = keySerializer?.Serialize(topic, message.Key);
            var valBytes = valueSerializer?.Serialize(topic, message.Value);

            producer.ProduceImpl(
                topic,
                valBytes, 0, valBytes == null ? 0 : valBytes.Length,
                keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length,
                message.Timestamp, Partition.Any,
                message.Headers, producer.blockIfQueueFullPropertyValue,
                new TypedDeliveryHandlerShim_Action(message.Key, message.Value, producer.enableDeliveryReportHeaderMarshaling, deliveryHandler)
            );
        }
    }
}
