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
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Impl;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Implements an Apache Kafka admin client.
    /// </summary>
    public class AdminClient : IAdminClient
    {
        private Task callbackTask;
        private CancellationTokenSource callbackCts;

        private IntPtr resultQueue = IntPtr.Zero;

        private const int POLL_TIMEOUT_MS = 100;
        private Task StartPollTask(CancellationToken ct)
            => Task.Factory.StartNew(() =>
                {
                    try
                    {
                        while (true)
                        {
                            ct.ThrowIfCancellationRequested();
                            
                            // TODO: probably also do this here.
                            // kafkaHandle.Poll((IntPtr)POLL_TIMEOUT_MS);

                            var eventPtr = kafkaHandle.QueuePoll(resultQueue, POLL_TIMEOUT_MS);
                            if (eventPtr == IntPtr.Zero)
                            {
                                continue;
                            }

                            int type = LibRdKafka.event_type(eventPtr);
                            // LibRdKafka.even
                        }
                    }
                    catch (OperationCanceledException) {}
                }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        /// <summary>
        ///     Options for the CreateTopics method.
        /// </summary>
        public class CreateTopicsOptions
        {
            /// <summary>
            ///     If true, the request should be validated only without creating the topic.
            /// 
            ///     Default: false
            /// </summary>
            public bool ValidateOnly { get; set; } = false;

            /// <summary>
            ///     The request timeout in milliseconds for this operation or null if the
            ///     default request timeout for the AdminClient should be used.
            /// 
            ///     Default: null
            /// </summary>
            public TimeSpan? Timeout { get; set; }
        }

        /// <summary>
        ///     Specification of a new topic to be created via the CreateTopics method.
        /// </summary>
        public class NewTopicSpecification
        {
            /// <summary>
            ///     The configuration to use on the new topic.
            /// </summary>
            public Dictionary<string, string> Configs { get; set; }

            /// <summary>
            ///     The name of the topic to be created (required)
            /// </summary>
            public string Name { get; set; }

            /// <summary>
            ///     The numer of partitions for the new topic or -1 if a replica assignment
            ///     is specified.
            /// </summary>
            public int NumPartitions { get; set; } = -1;

            /// <summary>
            ///     A map from partition id to replica ids (i.e.static broker ids) or null
            ///     if the number of partitions and replication factor are specified
            ///     instead.
            /// </summary>
            public Dictionary<int, List<int>> ReplicasAssignments { get; set; } = null;

            /// <summary>
            ///     The replication factor for the new topic or -1 if a replica assignment
            ///     is specified instead.
            /// </summary>
            public short ReplicationFactor { get; set; } = -1;
        }

        internal class AdminClientResult : TaskCompletionSource<object>
        {
        }

        /// <summary>
        ///     Create a new topic
        /// </summary>
        /// <param name="topic">
        ///     Specification of the new topic to create.
        /// </param>
        /// <param name="options">
        ///     The options to use when creating the new topic.
        /// </param>
        public Task<object> CreateTopic(NewTopicSpecification topic, CreateTopicsOptions options = null)
        {
            AdminClientResult completionSource = new AdminClientResult();
            
            var gch = GCHandle.Alloc(completionSource);
            var completionSourcePtr = GCHandle.ToIntPtr(gch);

            Handle.LibrdkafkaHandle.CreateTopics(topic, options, resultQueue, completionSourcePtr);

            return completionSource.Task;
        }

        /// <summary>
        ///     Create a batch of new topics.
        /// </summary>
        /// <param name="topics">
        ///     A collection os specifications of the new topics to create.
        /// </param>
        /// <param name="options">
        ///     The options to use when creating the new topics.
        /// </param>
        public void CreateTopics(IEnumerable<NewTopicSpecification> topics, CreateTopicsOptions options = null)
        {
            throw new NotImplementedException();
        }

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

            resultQueue = kafkaHandle.CreateQueue();

            callbackCts = new CancellationTokenSource();
            callbackTask = StartPollTask(callbackCts.Token);
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
            callbackCts.Cancel();
            try
            {
                callbackTask.Wait();
            }
            catch (AggregateException e)
            {
                if (e.InnerException.GetType() != typeof(TaskCanceledException))
                {
                    throw e.InnerException;
                }
            }
            finally
            {
                callbackCts.Dispose();
            }

            kafkaHandle.DestroyQueue(resultQueue);

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
