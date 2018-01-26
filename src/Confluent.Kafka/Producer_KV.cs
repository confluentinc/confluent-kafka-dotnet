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
    public class Producer<TKey, TValue> : IProducer<TKey, TValue>, IDisposable
    {
        private readonly Producer producer;
        private readonly ISerializingProducer<TKey, TValue> serializingProducer;

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
        /// <param name="manualPoll">
        ///     If true, does not start a dedicated polling thread to trigger events or receive delivery reports -
        ///     you must call the Poll method periodically instead. Typically you should set this parameter to false.
        /// </param>
        /// <param name="disableDeliveryReports">
        ///     If true, disables delivery report notification. Note: if set to true and you use a ProduceAsync variant that returns
        ///     a Task, the Tasks will never complete. Typically you should set this parameter to false. Set it to true for "fire and
        ///     forget" semantics and a small boost in performance.
        /// </param>
        private Producer(
            IEnumerable<KeyValuePair<string, object>> config,
            ISerializer<TKey> keySerializer,
            ISerializer<TValue> valueSerializer,
            bool manualPoll, bool disableDeliveryReports)
        {
            var configWithoutKeySerializerProperties = keySerializer?.Configure(config, true) ?? config;
            var configWithoutValueSerializerProperties = valueSerializer?.Configure(config, false) ?? config;

            var configWithoutSerializerProperties = config.Where(item => 
                configWithoutKeySerializerProperties.Any(ci => ci.Key == item.Key) &&
                configWithoutValueSerializerProperties.Any(ci => ci.Key == item.Key)
            );

            producer = new Producer(
                configWithoutSerializerProperties,
                manualPoll,
                disableDeliveryReports
            );

            serializingProducer = producer.GetSerializingProducer(keySerializer, valueSerializer);
        }

        /// <summary>
        ///     Initializes a new Producer instance.
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
            ISerializer<TValue> valueSerializer
        ) : this(config, keySerializer, valueSerializer, false, false) {}

        /// <include file='include_docs_producer.xml' path='API/Member[@name="KeySerializer"]/*' />
        public ISerializer<TKey> KeySerializer
            => serializingProducer.KeySerializer;

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ValueSerializer"]/*' />
        public ISerializer<TValue> ValueSerializer
            => serializingProducer.ValueSerializer;

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Client_Name"]/*' />
        public string Name
            => serializingProducer.Name;

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_topic_TKey_TValue"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Common"]/*' />
        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val)
            => serializingProducer.ProduceAsync(topic, key, val);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Message_TKey_TValue"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Common"]/*' />
        public Task<Message<TKey, TValue>> ProduceAsync(Message<TKey, TValue> message)
            => serializingProducer.ProduceAsync(message);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_Partition_TKey_TValue_Timestamp_IEnumerable"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Common"]/*' />
        public Task<Message<TKey, TValue>> ProduceAsync(
            string topic, Partition partition, 
            TKey key, TValue val, 
            Timestamp timestamp, IEnumerable<KeyValuePair<string, byte[]>> headers
        )
            => serializingProducer.ProduceAsync(topic, partition, key, val, timestamp, headers);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Message"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="Produce_IDeliveryHandler"]/*' />
        public void Produce(Message<TKey, TValue> message, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => serializingProducer.Produce(message, deliveryHandler);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_TKey_TValue"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="Produce_IDeliveryHandler"]/*' />
        public void Produce(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => serializingProducer.Produce(topic, key, val, deliveryHandler);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_Partition_TKey_TValue_Timestamp_IEnumerable"]/*' />        
        /// <include file='include_docs_producer.xml' path='API/Member[@name="Produce_IDeliveryHandler"]/*' />
        public void Produce(
            string topic, Partition partition, 
            TKey key, TValue val, 
            Timestamp timestamp, IEnumerable<KeyValuePair<string, byte[]>> headers, 
            IDeliveryHandler<TKey, TValue> deliveryHandler
        )
            => serializingProducer.Produce(topic, partition, key, val, timestamp, headers, deliveryHandler);

#region obsolete produce methods

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete("Variants of ProduceAsync that include a blockIfQueueFull parameter are depreciated - use the dotnet.producer.block.if.queue.full configuration property instead.")]
        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, int partition, bool blockIfQueueFull)
            => serializingProducer.ProduceAsync(topic, key, val, partition, blockIfQueueFull);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete("The Producer API has been revised and this overload of ProduceAsync has been depreciated. Please use another variant of ProduceAsync.")]
        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, int partition)
            => serializingProducer.ProduceAsync(topic, key, val, partition);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete("Variants of ProduceAsync that include a blockIfQueueFull parameter are depreciated - use the dotnet.producer.block.if.queue.full configuration property instead.")]
        public Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, bool blockIfQueueFull)
            => serializingProducer.ProduceAsync(topic, key, val, blockIfQueueFull);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete("Variants of ProduceAsync that include a IDeliveryHandler parameter are depreciated - use a variant of Produce instead. ")]
        public void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => serializingProducer.ProduceAsync(topic, key, val, deliveryHandler);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete(
            "Variants of ProduceAsync that include a IDeliveryHandler parameter are depreciated - use a variant of Produce instead. " +
            "Variants of ProduceAsync that include a blockIfQueueFull parameter are depreciated - use the dotnet.producer.block.if.queue.full configuration property instead.")]
        public void ProduceAsync(string topic, TKey key, TValue val, int partition, bool blockIfQueueFull, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => serializingProducer.ProduceAsync(topic, key, val, partition, blockIfQueueFull, deliveryHandler);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete("Variants of ProduceAsync that include a IDeliveryHandler parameter are depreciated - use a variant of Produce instead. ")]
        public void ProduceAsync(string topic, TKey key, TValue val, int partition, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => serializingProducer.ProduceAsync(topic, key, val, partition, deliveryHandler);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete(
            "Variants of ProduceAsync that include a IDeliveryHandler parameter are depreciated - use a variant of Produce instead. " +
            "Variants of ProduceAsync that include a blockIfQueueFull parameter are depreciated - use the dotnet.producer.block.if.queue.full configuration property instead.")]
        public void ProduceAsync(string topic, TKey key, TValue val, bool blockIfQueueFull, IDeliveryHandler<TKey, TValue> deliveryHandler)
            => serializingProducer.ProduceAsync(topic, key, val, blockIfQueueFull, deliveryHandler);

#endregion

        /// <include file='include_docs_client.xml' path='API/Member[@name="OnLog"]/*' />
        public event EventHandler<LogMessage> OnLog
        {
            add { producer.OnLog += value; }
            remove { producer.OnLog -= value; }
        }

        /// <include file='include_docs_client.xml' path='API/Member[@name="OnStatistics"]/*' />
        public event EventHandler<string> OnStatistics
        {
            add { producer.OnStatistics += value; }
            remove { producer.OnStatistics -= value; }
        }

        /// <include file='include_docs_producer.xml' path='API/Member[@name="OnError"]/*' />
        public event EventHandler<Error> OnError
        {
            add { producer.OnError += value; }
            remove { producer.OnError -= value; }
        }

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Flush_int"]/*' />
        public int Flush(int millisecondsTimeout)
            => producer.Flush(millisecondsTimeout);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Flush_TimeSpan"]/*' />
        public int Flush(TimeSpan timeout)
            => producer.Flush(timeout.TotalMillisecondsAsInt());

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Dispose"]/*' />
        public void Dispose()
        {
            if (KeySerializer != null)
            {
                KeySerializer.Dispose();
            }

            if (ValueSerializer != null)
            {
                ValueSerializer.Dispose();
            }

            producer.Dispose();
        }

        /// <include file='include_docs_client.xml' path='API/Member[@name="ListGroups"]/*' />
        public List<GroupInfo> ListGroups(TimeSpan timeout)
            => producer.ListGroups(timeout);

        /// <include file='include_docs_client.xml' path='API/Member[@name="ListGroup_string_TimeSpan"]/*' />
        public GroupInfo ListGroup(string group, TimeSpan timeout)
            => producer.ListGroup(group, timeout);

        /// <include file='include_docs_client.xml' path='API/Member[@name="ListGroup_string"]/*' />
        public GroupInfo ListGroup(string group)
            => producer.ListGroup(group);

        /// <include file='include_docs_client.xml' path='API/Member[@name="QueryWatermarkOffsets_TopicPartition_TimeSpan"]/*' />
        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
            => producer.QueryWatermarkOffsets(topicPartition, timeout);

        /// <include file='include_docs_client.xml' path='API/Member[@name="QueryWatermarkOffsets_TopicPartition"]/*' />
        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition)
            => producer.QueryWatermarkOffsets(topicPartition);

        /// <include file='include_docs_client.xml' path='API/Member[@name="GetMetadata_bool_string_TimeOut"]/*' />
        public Metadata GetMetadata(bool allTopics, string topic, TimeSpan timeout)
            => producer.GetMetadata(allTopics, topic, timeout);

        /// <include file='include_docs_client.xml' path='API/Member[@name="GetMetadata_bool_string"]/*' />
        public Metadata GetMetadata(bool allTopics, string topic)
            => producer.GetMetadata(allTopics, topic);

        /// <include file='include_docs_client.xml' path='API/Member[@name="AddBrokers_string"]/*' />
        public int AddBrokers(string brokers)
            => producer.AddBrokers(brokers);

    }
}