// Copyright 2016-2017 Confluent Inc.
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
using System.Threading.Tasks;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka
{
    /// <summary>
    ///     This interface describes the minimum functionality
    ///     to be provided by a high level (serializing) Kafka 
    ///     producer.
    /// </summary>
    public interface ISerializingProducer<TKey, TValue>
    {
        /// <summary>
        ///     Gets the name of the underlying producer instance.
        /// </summary>
        string Name { get; }

        /// <summary>
        ///     Gets the ISerializer implementation instance used to serialize keys.
        /// </summary>
        ISerializer<TKey> KeySerializer { get; }

        /// <summary>
        ///     Gets the ISerializer implementation instance used to serialize values.
        /// </summary>
        ISerializer<TValue> ValueSerializer { get; }

        /// <summary>
        ///     Asynchronously send a single message to the broker.
        /// </summary>
        /// <param name="topic">
        ///     The target topic.
        /// </param>
        /// <param name="key">
        ///     The message key (possibly null if allowed by the key serializer).
        /// </param>
        /// <param name="val">
        ///     The message value (possibly null if allowed by the value serializer).
        /// </param>
        /// <returns>
        ///     A Task which will complete with the corresponding delivery report
        ///     for this request.
        /// </returns>
        /// <remarks>
        ///     The partition the message is produced to is determined using the configured partitioner.
        ///     
        ///     Blocks if the send queue is full. Warning: if background polling is disabled and Poll is
        ///     not being called in another thread, this will block indefinitely.
        ///     
        ///     If you require strict ordering of delivery reports to be maintained, 
        ///     you should use a variant of ProduceAsync that takes an IDeliveryHandler
        ///     parameter, not a variant that returns a Task&lt;Message&gt; because 
        ///     Tasks are completed on arbitrary thread pool threads and can 
        ///     be executed out of order.
        /// </remarks>
        Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val);
        
        /// <summary>
        ///     Asynchronously send a single message to the broker.
        /// </summary>
        /// <param name="produceRecord">
        ///     The record informations.
        /// </param>
        /// <returns>
        ///     A Task which will complete with the corresponding delivery report
        ///     for this request.
        /// </returns>
        /// <remarks>
        ///     If you require strict ordering of delivery reports to be maintained, 
        ///     you should use a variant of ProduceAsync that takes an IDeliveryHandler
        ///     parameter, not a variant that returns a Task&lt;Message&gt; because 
        ///     Tasks are completed on arbitrary thread pool threads and can 
        ///     be executed out of order.
        /// </remarks>
        Task<Message<TKey, TValue>> ProduceAsync(ProduceRecord<TKey, TValue> produceRecord);
        
        /// <summary>
        ///     Asynchronously send a single message to the broker (order of delivery reports strictly guarenteed).
        /// </summary>
        /// <param name="topic">
        ///     The target topic.
        /// </param>
        /// <param name="key">
        ///     The message key (possibly null if allowed by the key serializer).
        /// </param>
        /// <param name="val">
        ///     The message value (possibly null if allowed by the value serializer).
        /// </param>
        /// <param name="deliveryHandler">
        ///     The handler where notification of deliverys is reported.
        ///     The order in which messages were acknowledged by the broker / failed is striclty guaranteed 
        ///     (failure may be via broker or local). 
        ///     IDeliveryHandler.HandleDeliveryReport callbacks are executed on the Poll thread.
        /// </param>
        /// <remarks>
        ///     The partition the message is produced to is determined using the configured partitioner.
        ///     
        ///     Blocks if the send queue is full. Warning: if background polling is disabled and Poll is
        ///     not being called in another thread, this will block indefinitely.
        /// </remarks>
        void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler);
        
        /// <summary>
        ///     Asynchronously send a single message to the broker (order of delivery reports strictly guarenteed).
        /// </summary>
        /// <param name="produceRecord">
        ///     The record informations.
        /// </param>
        /// <param name="deliveryHandler">
        ///     The handler where notification of deliverys is reported.
        ///     The order in which messages were acknowledged by the broker / failed is striclty guaranteed 
        ///     (failure may be via broker or local). 
        ///     IDeliveryHandler.HandleDeliveryReport callbacks are executed on the Poll thread.
        /// </param>
        void ProduceAsync(ProduceRecord<TKey, TValue> produceRecord, IDeliveryHandler<TKey, TValue> deliveryHandler);

        // TODO: remove in a later release.
        // Changing the Confluent.Kafka dll from <=0.9.4 to current without recompiling will then generate runtime exception
        // Current behavior generates compilation exception to get rid of those API
        [Obsolete("Use ProduceAsync(ProduceRecord) instead", true)]
        Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, int partition, bool blockIfQueueFull);
        [Obsolete("Use ProduceAsync(ProduceRecord) instead", true)]
        Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, int partition);
        [Obsolete("Use ProduceAsync(ProduceRecord) instead", true)]
        Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, bool blockIfQueueFull);
        [Obsolete("Use ProduceAsync(ProduceRecord, IDeliveryHandler) instead", true)]
        void ProduceAsync(string topic, TKey key, TValue val, int partition, bool blockIfQueueFull, IDeliveryHandler<TKey, TValue> deliveryHandler);
        [Obsolete("Use ProduceAsync(ProduceRecord, IDeliveryHandler) instead", true)]
        void ProduceAsync(string topic, TKey key, TValue val, int partition, IDeliveryHandler<TKey, TValue> deliveryHandler);
        [Obsolete("Use ProduceAsync(ProduceRecord, IDeliveryHandler) instead", true)]
        void ProduceAsync(string topic, TKey key, TValue val, bool blockIfQueueFull, IDeliveryHandler<TKey, TValue> deliveryHandler);

        /// <summary>
        ///     Raised when there is information that should be logged.
        /// </summary>
        /// <remarks>
        ///     You can specify the log level with the 'log_level' configuration
        ///     property. You also probably want to specify one or more debug
        ///     contexts using the 'debug' configuration property.
        ///
        ///     This event can potentially be raised on any thread.
        /// </remarks>
        event EventHandler<LogMessage> OnLog;

        /// <summary>
        ///     Raised on critical errors, e.g. connection failures or all brokers down.
        /// </summary>
        /// <remarks>
        ///     Called on the Producer poll thread.
        /// </remarks>
        event EventHandler<Error> OnError;

        /// <summary>
        ///     Raised on librdkafka statistics events. JSON formatted
        ///     string as defined here: https://github.com/edenhill/librdkafka/wiki/Statistics
        /// </summary>
        /// <remarks>
        ///     You can enable statistics and set the statistics interval
        ///     using the statistics.interval.ms configuration parameter
        ///     (disabled by default).
        ///
        ///     Called on the Producer poll thread.
        /// </remarks>
        event EventHandler<string> OnStatistics;
    }
}
