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
using System.Collections.Generic;
using System.Collections.Concurrent;
using Confluent.Kafka.Impl;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Implements an Apache Kafka admin client.
    /// </summary>
    public class AdminClient : IAdminClient
    {
        private IClient ownedClient;
        private Handle handle;

        private SafeKafkaHandle kafkaHandle
            => handle.LibrdkafkaHandle;

        private ConcurrentDictionary<string, SafeTopicHandle> topicHandles
            = new ConcurrentDictionary<string, SafeTopicHandle>(StringComparer.Ordinal);

        private Func<string, SafeTopicHandle> topicHandlerFactory;

        /// <summary>
        ///     Initialize a new AdminClient instance.
        /// </summary>
        /// <param name="config">
        ///     librdkafka configuration parameters (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
        /// </param>
        public AdminClient(IEnumerable<KeyValuePair<string, object>> config)
        {
            // TODO: Sort out exact behavior with poll. We don't really want to
            // have the capability to manually poll on this class because we
            // don't know if we're wrapping a consumer or producer.

            this.ownedClient = new Producer(config);
            this.handle = new Handle
            { 
                Owner = this,
                LibrdkafkaHandle = ownedClient.Handle.LibrdkafkaHandle
            };
            Init();
        }

        /// <summary>
        ///     Initialize a new AdminClient instance.
        /// </summary>
        /// <param name="handle">
        ///     An underlying librdkafka client handle to use to make broker requests.
        ///     It is valid to provide either a Consumer or Producer handle.
        /// </param>
        public AdminClient(Handle handle)
        {
            this.ownedClient = null;
            this.handle = handle;
            Init();
        }

        private void Init()
        {
            // note: ConcurrentDictionary.GetorAdd() method is not atomic
            this.topicHandlerFactory = (string topicName) =>
            {
                // Note: there is a possible (benign) race condition here - topicHandle could have already
                // been created for the topic (and possibly added to topicHandles). If the topicHandle has
                // already been created, rdkafka will return it and not create another. the call to rdkafka
                // is threadsafe.
                return kafkaHandle.Topic(topicName, IntPtr.Zero);
            };
        }

#region Groups
        /// <include file='include_docs_client.xml' path='API/Member[@name="ListGroups_TimeSpan"]/*' />
        public List<GroupInfo> ListGroups(TimeSpan timeout)
            => kafkaHandle.ListGroups(timeout.TotalMillisecondsAsInt());

        /// <include file='include_docs_client.xml' path='API/Member[@name="ListGroup_string_TimeSpan"]/*' />
        public GroupInfo ListGroup(string group, TimeSpan timeout)
            => kafkaHandle.ListGroup(group, timeout.TotalMillisecondsAsInt());

        /// <include file='include_docs_client.xml' path='API/Member[@name="ListGroup_string"]/*' />
        public GroupInfo ListGroup(string group)
            => kafkaHandle.ListGroup(group, -1);
#endregion

#region WatermarkOffsets
        /// <include file='include_docs_consumer.xml' path='API/Member[@name="GetWatermarkOffsets_TopicPartition"]/*' />
        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
            => kafkaHandle.GetWatermarkOffsets(topicPartition.Topic, topicPartition.Partition);

        /// <include file='include_docs_client.xml' path='API/Member[@name="QueryWatermarkOffsets_TopicPartition_TimeSpan"]/*' />
        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
            => kafkaHandle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, timeout.TotalMillisecondsAsInt());

        /// <include file='include_docs_client.xml' path='API/Member[@name="QueryWatermarkOffsets_TopicPartition"]/*' />
        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition)
            => kafkaHandle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, -1);
#endregion

#region Metadata
        private SafeTopicHandle getKafkaTopicHandle(string topic) 
            => topicHandles.GetOrAdd(topic, topicHandlerFactory);

        /// <include file='include_docs_client.xml' path='API/Member[@name="GetMetadata_bool_TimeSpan"]/*' />
        public Metadata GetMetadata(bool allTopics, TimeSpan timeout)
            => kafkaHandle.GetMetadata(allTopics, null, timeout.TotalMillisecondsAsInt());

        /// <summary>
        ///     Query the cluster for metadata (blocking).
        ///
        ///     - allTopics = true - request all topics from cluster
        ///     - allTopics = false, topic = null - request only locally known topics.
        ///     - allTopics = false, topic = valid - request specific topic
        /// </summary>
        public Metadata GetMetadata(bool allTopics, string topic, TimeSpan timeout)
            => kafkaHandle.GetMetadata(
                allTopics, 
                topic == null ? null : getKafkaTopicHandle(topic), 
                timeout.TotalMillisecondsAsInt());
#endregion

        /// <include file='include_docs_client.xml' path='API/Member[@name="AddBrokers_string"]/*' />
        public int AddBrokers(string brokers)
            => kafkaHandle.AddBrokers(brokers);

        /// <include file='include_docs_client.xml' path='API/Member[@name="Client_Name"]/*' />
        public string Name
            => kafkaHandle.Name;

        /// <summary>
        ///     An opaque reference to the underlying librdkafka client instance.
        /// </summary>
        public Handle Handle
            => handle;

        /// <summary>
        ///     Releases all resources used by this client.
        /// </summary>
        public void Dispose()
        {
            foreach (var kv in topicHandles)
            {
                kv.Value.Dispose();
            }

            if (handle.Owner == this)
            {
                ownedClient.Dispose();
            }
        }

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
    }
}
