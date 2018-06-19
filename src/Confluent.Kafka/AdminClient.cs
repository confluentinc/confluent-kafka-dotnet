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
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Impl;
using static Confluent.Kafka.Internal.Util.Marshal;


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

        private Dictionary<string, Error> extractTopicResults(IntPtr topicResultsPtr, int topicResultsCount)
        {
            IntPtr[] topicResultsPtrArr = new IntPtr[topicResultsCount];
            Marshal.Copy(topicResultsPtr, topicResultsPtrArr, 0, topicResultsCount);

            var topicResults = topicResultsPtrArr.Select(topicResultPtr => new { 
                Name = PtrToStringUTF8(LibRdKafka.topic_result_name(topicResultPtr)),
                ErrorCode = LibRdKafka.topic_result_error(topicResultPtr),
                ErrorReason = PtrToStringUTF8(LibRdKafka.topic_result_error_string(topicResultPtr))
            });
            
            return topicResults.ToDictionary(x => x.Name, x => new Error(x.ErrorCode, x.ErrorReason));
        }

        private ConfigEntry extractConfigEntry(IntPtr configEntryPtr)
        {
            Dictionary<string, ConfigEntry> synonyms = null;
            var synonymsPtr = LibRdKafka.ConfigEntry_synonyms(configEntryPtr, out UIntPtr synonymsCount);
            if (synonymsPtr != IntPtr.Zero)
            {
                IntPtr[] synonymsPtrArr = new IntPtr[(int)synonymsCount];
                Marshal.Copy(synonymsPtr, synonymsPtrArr, 0, (int)synonymsCount);
                synonyms = synonymsPtrArr
                    .Select(synonymPtr => extractConfigEntry(synonymPtr))
                    .ToDictionary(x => x.Name, x => x);
            }

            return new ConfigEntry
            {
                Name = PtrToStringUTF8(LibRdKafka.ConfigEntry_name(configEntryPtr)),
                Value = PtrToStringUTF8(LibRdKafka.ConfigEntry_value(configEntryPtr)),
                IsDefault = (int)LibRdKafka.ConfigEntry_is_default(configEntryPtr) == 1,
                IsSensitive = (int)LibRdKafka.ConfigEntry_is_sensitive(configEntryPtr) == 1,
                IsReadOnly = (int)LibRdKafka.ConfigEntry_is_read_only(configEntryPtr) == 1,
                IsSynonym = (int)LibRdKafka.ConfigEntry_is_synonym(configEntryPtr) == 1,
                // Source = (ConfigSource)(IntPtr)LibRdKafka.ConfigEntry_source(configEntryPtr), doesn't work!
                Synonyms = synonyms
            };
        }

        private Dictionary<ConfigResource, ConfigResult> extractResultConfigs(IntPtr configResourcesPtr, int configResourceCount)
        {
            var result = new Dictionary<ConfigResource, ConfigResult>();

            IntPtr[] configResourcesPtrArr = new IntPtr[configResourceCount];
            Marshal.Copy(configResourcesPtr, configResourcesPtrArr, 0, configResourceCount);
            foreach (var configResourcePtr in configResourcesPtrArr)
            {
                var resourceName = PtrToStringUTF8(LibRdKafka.ConfigResource_name(configResourcePtr));
                var errorCode = LibRdKafka.ConfigResource_error(configResourcePtr);
                var errorReason = PtrToStringUTF8(LibRdKafka.ConfigResource_error_string(configResourcePtr));
                var resourceConfigType = LibRdKafka.ConfigResource_type(configResourcePtr);

                var configEntriesPtr = LibRdKafka.ConfigResource_configs(configResourcePtr, out UIntPtr configEntryCount);
                IntPtr[] configEntriesPtrArr = new IntPtr[(int)configEntryCount];
                Marshal.Copy(configEntriesPtr, configEntriesPtrArr, 0, (int)configEntryCount);
                var configEntries = configEntriesPtrArr
                    .Select(configEntryPtr => extractConfigEntry(configEntryPtr))
                    .ToList();

                result.Add(
                    new ConfigResource { Name = resourceName, ResourceType = resourceConfigType },
                    new ConfigResult { Entries = configEntries, Error = new Error(errorCode, errorReason) });
            }

            return result;
        }

        private const int POLL_TIMEOUT_MS = 100;
        private Task StartPollTask(CancellationToken ct)
            => Task.Factory.StartNew(() =>
                {
                    try
                    {
                        while (true)
                        {
                            ct.ThrowIfCancellationRequested();

                            var eventPtr = kafkaHandle.QueuePoll(resultQueue, POLL_TIMEOUT_MS);
                            if (eventPtr == IntPtr.Zero)
                            {
                                continue;
                            }

                            var type = LibRdKafka.event_type(eventPtr);

                            var ptr = (IntPtr)LibRdKafka.event_opaque(eventPtr);
                            var gch = GCHandle.FromIntPtr(ptr);
                            var adminClientResult = gch.Target;
                            gch.Free();

                            if (!adminClientResultTypes.TryGetValue(type, out Type expectedType))
                            {
                                // Should never happen.
                                throw new InvalidOperationException($"Unknown result type: {type}");
                            }

                            if (adminClientResult.GetType() != expectedType)
                            {
                                // Should never happen.
                                throw new InvalidOperationException($"Completion source type mismatch. Exected {expectedType.Name}, got {type}");
                            }

                            var errorCode = LibRdKafka.event_error(eventPtr);
                            var errorStr = LibRdKafka.event_error_string(eventPtr);

                            switch (type)
                            {
                                case LibRdKafka.EventType.CreateTopics_Result:
                                    {
                                        if (errorCode != ErrorCode.NoError)
                                        {
                                            ((TaskCompletionSource<Dictionary<string, Error>>)adminClientResult).SetException(new KafkaException(new Error(errorCode, errorStr)));
                                            return;
                                        }
                                        ((TaskCompletionSource<Dictionary<string, Error>>)adminClientResult).SetResult(
                                            extractTopicResults(
                                                LibRdKafka.CreateTopics_result_topics(eventPtr, out UIntPtr resultCountPtr), (int)resultCountPtr));
                                    }
                                    break;

                                case LibRdKafka.EventType.DeleteTopics_Result:
                                    {
                                        if (errorCode != ErrorCode.NoError)
                                        {
                                            ((TaskCompletionSource<Dictionary<string, Error>>)adminClientResult).SetException(new KafkaException(new Error(errorCode, errorStr)));
                                            return;
                                        }
                                        ((TaskCompletionSource<Dictionary<string, Error>>)adminClientResult).SetResult(
                                            extractTopicResults(
                                                LibRdKafka.DeleteTopics_result_topics(eventPtr, out UIntPtr resultCountPtr), (int)resultCountPtr));
                                    }
                                    break;

                                case LibRdKafka.EventType.CreatePartitions_Result:
                                    {
                                        if (errorCode != ErrorCode.NoError)
                                        {
                                            ((TaskCompletionSource<Dictionary<string, Error>>)adminClientResult).SetException(new KafkaException(new Error(errorCode, errorStr)));
                                            return;
                                        }
                                        ((TaskCompletionSource<Dictionary<string, Error>>)adminClientResult).SetResult(
                                            extractTopicResults(
                                                LibRdKafka.CreatePartitions_result_topics(eventPtr, out UIntPtr resultCountPtr), (int)resultCountPtr));
                                    }
                                    break;

                                case LibRdKafka.EventType.DescribeConfigs_Result:
                                    {
                                        if (errorCode != ErrorCode.NoError)
                                        {
                                            ((TaskCompletionSource<Dictionary<ConfigResource, ConfigResult>>)adminClientResult).SetException(new KafkaException(new Error(errorCode, errorStr)));
                                            return;
                                        }
                                        ((TaskCompletionSource<Dictionary<ConfigResource, ConfigResult>>)adminClientResult).SetResult(
                                            extractResultConfigs(
                                                LibRdKafka.DescribeConfigs_result_resources(eventPtr, out UIntPtr cntp), (int)cntp));
                                    }
                                    break;

                                case LibRdKafka.EventType.AlterConfigs_Result:
                                    {
                                        if (errorCode != ErrorCode.NoError)
                                        {
                                            ((TaskCompletionSource<Dictionary<ConfigResource, ConfigResult>>)adminClientResult).SetException(new KafkaException(new Error(errorCode, errorStr)));
                                            return;
                                        }
                                        ((TaskCompletionSource<Dictionary<ConfigResource, ConfigResult>>)adminClientResult).SetResult(
                                            extractResultConfigs(
                                                LibRdKafka.AlterConfigs_result_resources(eventPtr, out UIntPtr cntp), (int)cntp));
                                    }
                                    break;

                                default:
                                    // Should never happen.
                                    throw new InvalidOperationException($"Unknown result type: {type}");
                            }
                        }
                    }
                    catch (OperationCanceledException) {}
                }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);


        internal static Dictionary<LibRdKafka.EventType, Type> adminClientResultTypes = new Dictionary<LibRdKafka.EventType, Type>
        {
            { LibRdKafka.EventType.CreateTopics_Result, typeof(TaskCompletionSource<List<CreateTopicResult>>) },
            { LibRdKafka.EventType.DeleteTopics_Result, typeof(TaskCompletionSource<List<DeleteTopicResult>>) },
            { LibRdKafka.EventType.DescribeConfigs_Result, typeof(TaskCompletionSource<List<DescribeConfigResult>>) },
            { LibRdKafka.EventType.AlterConfigs_Result, typeof(TaskCompletionSource<List<AlterConfigResult>>) },
            { LibRdKafka.EventType.CreatePartitions_Result, typeof(TaskCompletionSource<List<CreatePartitionResult>>) }
        };

        /// <summary>
        ///     Get the configuration for the specified resources. The returned 
        ///     configuration includes default values and the isDefault() method
        ///     can be used to distinguish them from user supplied values. The 
        ///     value of config entries where isSensitive() is true is always null 
        ///     so that sensitive information is not disclosed. Config entries where
        ///     isReadOnly() is true cannot be updated. This operation is supported 
        ///     by brokers with version 0.11.0.0 or higher.
        /// </summary>
        /// <param name="resources">
        ///     the resources (topic and broker resource types are currently supported)
        /// </param>
        /// <param name="options">
        ///     the options to use when describing configs.
        /// </param>
        /// <returns>
        ///     The D
        /// </returns>
        public Task<List<DescribeConfigResult>> DescribeConfigsAsync(IEnumerable<ConfigResource> resources, DescribeConfigsOptions options = null)
        {
            // TODO: To support results that may complete at different times, we may also want to implement:
            // List<Task<DescribeConfigResult>> DescribeConfigsConcurrent(IEnumerable<ConfigResource> resources, DescribeConfigsOptions options = null)

            var completionSource = new TaskCompletionSource<List<DescribeConfigResult>>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.DescribeConfigs(
                resources, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Update the configuration for the specified resources with the default options. 
        ///     Updates are not transactional so they may succeed for some resources while fail 
        ///     for others. The configs for a particular resource are updated atomically. This 
        ///     operation is supported by brokers with version 0.11.0.0 or higher.
        /// </summary>
        /// <param name="configs">
        ///     The resources with their configs (topic is the only resource type with configs
        ///     that can be updated currently)
        /// </param>
        /// <param name="options">
        ///     The options to use when altering configs.
        /// </param>
        /// <returns>
        ///     The Config associated with the specified 
        /// </returns>
        public Task<List<AlterConfigResult>> AlterConfigsAsync(Dictionary<ConfigResource, IEnumerable<ConfigEntry>> configs, AlterConfigsOptions options = null)
        {
            // TODO: To support results that may complete at different times, we may also want to implement:
            // List<Task<AlterConfigResult>> AlterConfigsConcurrent(Dictionary<ConfigResource, Config> configs, AlterConfigsOptions options = null)

            var completionSource = new TaskCompletionSource<List<AlterConfigResult>>();
            // Note: There is a level of indirection between the GCHandle and
            // physical memory address. GCHangle.ToIntPtr doesn't return the
            // physical address, it returns an id that refers to the object via
            // a handle-table.
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.AlterConfigs(
                configs, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Create a batch of new topics.
        /// </summary>
        /// <param name="topics">
        ///     A collection of specifications of the new topics to create.
        /// </param>
        /// <param name="options">
        ///     The options to use when creating the new topics.
        /// </param>
        public Task<List<CreateTopicResult>> CreateTopicsAsync(IEnumerable<NewTopic> topics, CreateTopicsOptions options = null)
        {
            // TODO: To support results that may complete at different times, we may also want to implement:
            // public List<Task<CreateTopicResult>> CreateTopicsConcurrent(IEnumerable<NewTopic> topics, CreateTopicsOptions options = null)

            var completionSource = new TaskCompletionSource<List<CreateTopicResult>>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.CreateTopics(
                topics, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Delete a batch of topics. This operation is not transactional so it may succeed for some topics while fail
        ///     for others. It may take several seconds after the DeleteTopicsResult returns success for all the brokers to
        ///     become aware that the topics are gone. During this time, AdminClient#listTopics and AdminClient#describeTopics
        ///     may continue to return information about the deleted topics. If delete.topic.enable is false on the brokers,
        ///     deleteTopics will mark the topics for deletion, but not actually delete them. The futures will return 
        ///     successfully in this case.
        /// </summary>
        /// <param name="topics">
        ///     The topic names to delete.
        /// </param>
        /// <param name="options">
        ///     The options to use when deleting topics.
        /// </param>
        /// <returns>
        ///     
        /// </returns>
        public Task<List<DeleteTopicResult>> DeleteTopicsAsync(IEnumerable<string> topics, DeleteTopicsOptions options = null)
        {
            // TODO: To support results that may complete at different times, we may also want to implement:
            // List<Task<DeleteTopicResult>> DeleteTopicsConcurrent(IEnumerable<string> topics, DeleteTopicsOptions options = null)

            var completionSource = new TaskCompletionSource<List<DeleteTopicResult>>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.DeleteTopics(
                topics, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Increase the number of partitions of the topics specified as the keys of newPartitions according to the corresponding values.
        /// </summary>
        /// <param name="newPartitions">
        ///     A collection of specifications of the new partitions to create for each topic.
        /// </param>
        /// <param name="options">
        ///     The options to use when creating the partitions.
        /// </param>
        /// <returns></returns>
        public Task<List<CreatePartitionResult>> CreatePartitionsAsync(IDictionary<string, NewPartitions> newPartitions, CreatePartitionsOptions options = null)
        {
            // TODO: To support results that may complete at different times, we may also want to implement:
            // List<Task<CreatePartitionResult>> CreatePartitionsAsync(IDictionary<string, NewPartitions> newPartitions, CreatePartitionsOptions options = null)

            var completionSource = new TaskCompletionSource<List<CreatePartitionResult>>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.CreatePartitions(
                newPartitions, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
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
