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
    /// <summary>
    ///     Defines a high-level Apache Kafka producer (without serialization).
    /// </summary>
    public interface IProducer : IDisposable
    {
        /// <include file='include_docs_producer.xml' path='API/Member[@name="Poll_int"]/*' />
        int Poll(int millisecondsTimeout);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Poll_TimeSpan"]/*' />
        int Poll(TimeSpan timeout);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Poll"]/*' />
        [Obsolete("Use an overload of Poll with a finite timeout.", false)]
        int Poll();

        /// <include file='include_docs_producer.xml' path='API/Member[@name="GetSerializingProducer"]/*' />
        ISerializingProducer<TKey, TValue> GetSerializingProducer<TKey, TValue>(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Message"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Common"]/*' />
        Task<Message> ProduceAsync(Message message);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_TKey_TValue"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Common"]/*' />
        Task<Message> ProduceAsync(string topic, byte[] key, byte[] val);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_Partition_TKey_TValue_Timestamp_IEnumerable"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Common"]/*' />
        Task<Message> ProduceAsync(
            string topic, Partition partition, 
            byte[] key, byte[] val, 
            Timestamp timestamp, 
            IEnumerable<KeyValuePair<string, byte[]>> headers);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_Partition_byte_int_int_byte_int_int_Timestamp_IEnumerable"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Common"]/*' />
        Task<Message> ProduceAsync(
            string topic, Partition partition, 
            byte[] key, int keyOffset, int keyLength, 
            byte[] val, int valOffset, int valLength, 
            Timestamp timestamp, IEnumerable<KeyValuePair<string, byte[]>> headers);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Message"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="Produce_IDeliveryHandler"]/*' />
        void Produce(Message message, IDeliveryHandler deliveryHandler);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_TKey_TValue"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="Produce_IDeliveryHandler"]/*' />
        void Produce(string topic, byte[] key, byte[] val, IDeliveryHandler deliveryHandler);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_Partition_TKey_TValue_Timestamp_IEnumerable"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="Produce_IDeliveryHandler"]/*' />
        void Produce(
            string topic, Partition partition,
            byte[] key, byte[] val,
            Timestamp timestamp,
            IEnumerable<KeyValuePair<string, byte[]>> headers,
            IDeliveryHandler deliveryHandler);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_Partition_byte_int_int_byte_int_int_Timestamp_IEnumerable"]/*' />        
        /// <include file='include_docs_producer.xml' path='API/Member[@name="Produce_IDeliveryHandler"]/*' />
        void Produce(
            string topic, Partition partition, 
            byte[] key, int keyOffset, int keyLength,
            byte[] val, int valOffset, int valLength,
            Timestamp timestamp, IEnumerable<KeyValuePair<string, byte[]>> headers,
            IDeliveryHandler deliveryHandler);

        /// <include file='include_docs_client.xml' path='API/Member[@name="Name"]/*' />
        string Name { get; }

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Flush_int"]/*' />
        int Flush(int millisecondsTimeout);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Flush_TimeSpan"]/*' />
        int Flush(TimeSpan timeout);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Flush"]/*' />
        [Obsolete("Use an overload of Flush with a finite timeout.", false)]
        int Flush();

        /// <include file='include_docs_client.xml' path='API/Member[@name="ListGroups"]/*' />
        List<GroupInfo> ListGroups(TimeSpan timeout);

        /// <include file='include_docs_client.xml' path='API/Member[@name="ListGroup_string_TimeSpan"]/*' />   
        GroupInfo ListGroup(string group, TimeSpan timeout);

        /// <include file='include_docs_client.xml' path='API/Member[@name="ListGroup_string"]/*' />   
        GroupInfo ListGroup(string group);

        /// <include file='include_docs_client.xml' path='API/Member[@name="QueryWatermarkOffsets_TopicPartition_TimeSpan"]/*' />   
        WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout);

        /// <include file='include_docs_client.xml' path='API/Member[@name="QueryWatermarkOffsets_TopicPartition"]/*' />  
        WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition);

        /// <include file='include_docs_client.xml' path='API/Member[@name="GetMetadata_bool_string_TimeSpan"]/*' />  
        Metadata GetMetadata(bool allTopics, string topic, TimeSpan timeout);

        /// <include file='include_docs_client.xml' path='API/Member[@name="GetMetadata_bool_string"]/*' />  
        Metadata GetMetadata(bool allTopics, string topic);

        /// <include file='include_docs_client.xml' path='API/Member[@name="GetMetadata"]/*' />  
        Metadata GetMetadata();

        /// <include file='include_docs_client.xml' path='API/Member[@name="AddBrokers_string"]/*' />  
        int AddBrokers(string brokers);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="OnError"]/*' />
        event EventHandler<Error> OnError;

        /// <include file='include_docs_client.xml' path='API/Member[@name="OnStatistics"]/*' />
        event EventHandler<string> OnStatistics;

        /// <include file='include_docs_client.xml' path='API/Member[@name="OnLog"]/*' />
        event EventHandler<LogMessage> OnLog;
    }
}
