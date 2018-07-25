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

        private List<CreateTopicResult> extractTopicResults(IntPtr topicResultsPtr, int topicResultsCount)
        {
            IntPtr[] topicResultsPtrArr = new IntPtr[topicResultsCount];
            Marshal.Copy(topicResultsPtr, topicResultsPtrArr, 0, topicResultsCount);

            return topicResultsPtrArr.Select(topicResultPtr => new CreateTopicResult 
                {
                    Topic = PtrToStringUTF8(Librdkafka.topic_result_name(topicResultPtr)),
                    Error = new Error(
                        Librdkafka.topic_result_error(topicResultPtr), 
                        PtrToStringUTF8(Librdkafka.topic_result_error_string(topicResultPtr)))
                }).ToList();
        }

        private ConfigEntryResult extractConfigEntry(IntPtr configEntryPtr)
        {
            var synonyms = new List<ConfigSynonym>();
            var synonymsPtr = Librdkafka.ConfigEntry_synonyms(configEntryPtr, out UIntPtr synonymsCount);
            if (synonymsPtr != IntPtr.Zero)
            {
                IntPtr[] synonymsPtrArr = new IntPtr[(int)synonymsCount];
                Marshal.Copy(synonymsPtr, synonymsPtrArr, 0, (int)synonymsCount);
                synonyms = synonymsPtrArr
                    .Select(synonymPtr => extractConfigEntry(synonymPtr))
                    .Select(e => new ConfigSynonym { Name = e.Name, Value = e.Value, Source = e.Source } )
                    .ToList();
            }

            return new ConfigEntryResult
            {
                Name = PtrToStringUTF8(Librdkafka.ConfigEntry_name(configEntryPtr)),
                Value = PtrToStringUTF8(Librdkafka.ConfigEntry_value(configEntryPtr)),
                IsDefault = (int)Librdkafka.ConfigEntry_is_default(configEntryPtr) == 1,
                IsSensitive = (int)Librdkafka.ConfigEntry_is_sensitive(configEntryPtr) == 1,
                IsReadOnly = (int)Librdkafka.ConfigEntry_is_read_only(configEntryPtr) == 1,
                Source = Librdkafka.ConfigEntry_source(configEntryPtr),
                Synonyms = synonyms
            };
        }

        private List<DescribeConfigResult> extractResultConfigs(IntPtr configResourcesPtr, int configResourceCount)
        {
            var result = new List<DescribeConfigResult>();

            IntPtr[] configResourcesPtrArr = new IntPtr[configResourceCount];
            Marshal.Copy(configResourcesPtr, configResourcesPtrArr, 0, configResourceCount);
            foreach (var configResourcePtr in configResourcesPtrArr)
            {
                var resourceName = PtrToStringUTF8(Librdkafka.ConfigResource_name(configResourcePtr));
                var errorCode = Librdkafka.ConfigResource_error(configResourcePtr);
                var errorReason = PtrToStringUTF8(Librdkafka.ConfigResource_error_string(configResourcePtr));
                var resourceConfigType = Librdkafka.ConfigResource_type(configResourcePtr);

                var configEntriesPtr = Librdkafka.ConfigResource_configs(configResourcePtr, out UIntPtr configEntryCount);
                IntPtr[] configEntriesPtrArr = new IntPtr[(int)configEntryCount];
                if ((int)configEntryCount > 0)
                {
                    Marshal.Copy(configEntriesPtr, configEntriesPtrArr, 0, (int)configEntryCount);
                }
                var configEntries = configEntriesPtrArr
                    .Select(configEntryPtr => extractConfigEntry(configEntryPtr))
                    .ToDictionary(e => e.Name);

                result.Add(new DescribeConfigResult { 
                    ConfigResource = new ConfigResource { Name = resourceName, Type = resourceConfigType },
                    Entries = configEntries,
                    Error = new Error(errorCode, errorReason)
                });
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

                            try
                            {
                                var eventPtr = kafkaHandle.QueuePoll(resultQueue, POLL_TIMEOUT_MS);
                                if (eventPtr == IntPtr.Zero)
                                {
                                    continue;
                                }

                                var type = Librdkafka.event_type(eventPtr);

                                var ptr = (IntPtr)Librdkafka.event_opaque(eventPtr);
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

                                var errorCode = Librdkafka.event_error(eventPtr);
                                var errorStr = Librdkafka.event_error_string(eventPtr);

                                switch (type)
                                {
                                    case Librdkafka.EventType.CreateTopics_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                ((TaskCompletionSource<List<CreateTopicResult>>)adminClientResult).TrySetException(new KafkaException(new Error(errorCode, errorStr)));
                                                return;
                                            }

                                            var result = extractTopicResults(
                                                Librdkafka.CreateTopics_result_topics(eventPtr, out UIntPtr resultCountPtr), (int)resultCountPtr);

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                ((TaskCompletionSource<List<CreateTopicResult>>)adminClientResult).TrySetException(new CreateTopicsException(result));
                                            }
                                            else
                                            {
                                                ((TaskCompletionSource<List<CreateTopicResult>>)adminClientResult).TrySetResult(result);
                                            }
                                        }
                                        break;

                                    case Librdkafka.EventType.DeleteTopics_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                ((TaskCompletionSource<List<DeleteTopicResult>>)adminClientResult).TrySetException(new KafkaException(new Error(errorCode, errorStr)));
                                                return;
                                            }

                                            var result = extractTopicResults(
                                                Librdkafka.DeleteTopics_result_topics(eventPtr, out UIntPtr resultCountPtr), (int)resultCountPtr)
                                                    .Select(r => new DeleteTopicResult { Topic = r.Topic, Error = r.Error }).ToList();

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                ((TaskCompletionSource<List<DeleteTopicResult>>)adminClientResult).TrySetException(new DeleteTopicsException(result));
                                            }
                                            else
                                            {
                                                ((TaskCompletionSource<List<DeleteTopicResult>>)adminClientResult).TrySetResult(result);
                                            }
                                        }
                                        break;

                                    case Librdkafka.EventType.CreatePartitions_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                ((TaskCompletionSource<List<CreatePartitionsResult>>)adminClientResult).TrySetException(new KafkaException(new Error(errorCode, errorStr)));
                                                return;
                                            }

                                            var result = extractTopicResults(
                                                    Librdkafka.CreatePartitions_result_topics(eventPtr, out UIntPtr resultCountPtr), (int)resultCountPtr)
                                                        .Select(r => new CreatePartitionsResult { Topic = r.Topic, Error = r.Error }).ToList();

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                ((TaskCompletionSource<List<CreatePartitionsResult>>)adminClientResult).TrySetException(new CreatePartitionsException(result));
                                            }
                                            else
                                            {
                                                ((TaskCompletionSource<List<CreatePartitionsResult>>)adminClientResult).TrySetResult(result);
                                            }
                                        }
                                        break;

                                    case Librdkafka.EventType.DescribeConfigs_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                ((TaskCompletionSource<List<DescribeConfigResult>>)adminClientResult).TrySetException(new KafkaException(new Error(errorCode, errorStr)));
                                                return;
                                            }

                                            var result = extractResultConfigs(
                                                Librdkafka.DescribeConfigs_result_resources(eventPtr, out UIntPtr cntp), (int)cntp);

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                ((TaskCompletionSource<List<DescribeConfigResult>>)adminClientResult).TrySetException(new DescribeConfigsException(result));
                                            }
                                            else
                                            {
                                                ((TaskCompletionSource<List<DescribeConfigResult>>)adminClientResult).TrySetResult(result);
                                            }
                                        }
                                        break;

                                    case Librdkafka.EventType.AlterConfigs_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                ((TaskCompletionSource<List<AlterConfigResult>>)adminClientResult).TrySetException(new KafkaException(new Error(errorCode, errorStr)));
                                                return;
                                            }

                                            var result = extractResultConfigs(
                                                Librdkafka.AlterConfigs_result_resources(eventPtr, out UIntPtr cntp), (int)cntp)
                                                    .Select(r => new AlterConfigResult { ConfigResource = r.ConfigResource, Error = r.Error }).ToList();

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                ((TaskCompletionSource<List<AlterConfigResult>>)adminClientResult).TrySetException(new AlterConfigsException(result));
                                            }
                                            else
                                            {
                                                ((TaskCompletionSource<List<AlterConfigResult>>) adminClientResult).TrySetResult(result);
                                            }
                                        }
                                        break;

                                    default:
                                        // Should never happen.
                                        throw new InvalidOperationException($"Unknown result type: {type}");
                                }
                            }
                            catch
                            {
                                // TODO: If this occurs, it means there's an application logic error
                                //       (i.e. program execution should never get here). Rather than
                                //       ignore the situation, we panic, destroy the librdkafka handle, 
                                //       and exit the polling loop. Further usage of the AdminClient will
                                //       result in exceptions. People will be sure to notice and tell us.
                                this.DisposeResources();
                            }
                        }
                    }
                    catch (OperationCanceledException) {}
                }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);


        internal static Dictionary<Librdkafka.EventType, Type> adminClientResultTypes = new Dictionary<Librdkafka.EventType, Type>
        {
            { Librdkafka.EventType.CreateTopics_Result, typeof(TaskCompletionSource<List<CreateTopicResult>>) },
            { Librdkafka.EventType.DeleteTopics_Result, typeof(TaskCompletionSource<List<DeleteTopicResult>>) },
            { Librdkafka.EventType.DescribeConfigs_Result, typeof(TaskCompletionSource<List<DescribeConfigResult>>) },
            { Librdkafka.EventType.AlterConfigs_Result, typeof(TaskCompletionSource<List<AlterConfigResult>>) },
            { Librdkafka.EventType.CreatePartitions_Result, typeof(TaskCompletionSource<List<CreatePartitionsResult>>) }
        };

        /// <summary>
        ///     Get the configuration for the specified resources. The returned 
        ///     configuration includes default values and the IsDefault property
        ///     can be used to distinguish them from user supplied values. The 
        ///     value of config entries where IsSensitive is true is always null 
        ///     so that sensitive information is not disclosed. Config entries where
        ///     IsReadOnly is true cannot be updated. This operation is supported 
        ///     by brokers with version 0.11.0.0 or higher.
        /// </summary>
        /// <param name="resources">
        ///     The resources (topic and broker resource types are currently 
        ///     supported)
        /// </param>
        /// <param name="options">
        ///     The options to use when describing configs.
        /// </param>
        /// <returns>
        ///     Configs for the specified resources.
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
        ///     Update the configuration for the specified resources. Updates are not transactional
        ///     so they may succeed for some resources while fail for others. The configs for a 
        ///     particular resource are updated atomically. This operation is supported by brokers 
        ///     with version 0.11.0.0 or higher.
        /// </summary>
        /// <param name="configs">
        ///     The resources with their configs (topic is the only resource type with configs
        ///     that can be updated currently)
        /// </param>
        /// <param name="options">
        ///     The options to use when altering configs.
        /// </param>
        /// <returns>
        ///     The results of the alter configs requests.
        /// </returns>
        public Task<List<AlterConfigResult>> AlterConfigsAsync(Dictionary<ConfigResource, List<ConfigEntry>> configs, AlterConfigsOptions options = null)
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
        ///     The options to use when creating the topics.
        /// </param>
        /// <returns>
        ///     The results of the create topic requests.
        /// </returns>
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
        ///     become aware that the topics are gone. During this time, topics may continue to be visible via admin 
        ///     operations. If delete.topic.enable is false on the brokers, DeleteTopicsAsync will mark the topics for
        ///     deletion, but not actually delete them. The futures will return successfully in this case.
        /// </summary>
        /// <param name="topics">
        ///     The topic names to delete.
        /// </param>
        /// <param name="options">
        ///     The options to use when deleting topics.
        /// </param>
        /// <returns>
        ///     The results of the delete topic requests.
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
        ///     Increase the number of partitions for one or more topics as per
        ///     the supplied NewPartitions specifications.
        /// </summary>
        /// <param name="newPartitions">
        ///     A collection of NewPartitions specifications.
        /// </param>
        /// <param name="options">
        ///     The options to use when creating the partitions.
        /// </param>
        /// <returns>
        ///     The results of the NewPartitions requests.
        /// </returns>
        public Task<List<CreatePartitionsResult>> CreatePartitionsAsync(
            IEnumerable<NewPartitions> newPartitions, CreatePartitionsOptions options = null)
        {
            // TODO: To support results that may complete at different times, we may also want to implement:
            // List<Task<CreatePartitionResult>> CreatePartitionsConcurrent(IEnumerable<NewPartitions> newPartitions, CreatePartitionsOptions options = null)

            var completionSource = new TaskCompletionSource<List<CreatePartitionsResult>>();
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
        ///     A collection of librdkafka configuration parameters 
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     and parameters specific to this client (refer to: 
        ///     <see cref="Confluent.Kafka.ConfigPropertyNames" />)
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

        /// <summary>
        ///     Get last known low (oldest available/beginning) and high (newest/end)
        ///     offsets for a topic/partition.
        /// </summary>
        /// <remarks>
        ///     This method is only avilable on instances constructed from a Consumer
        ///     handle. The low offset is updated periodically (if statistics.interval.ms 
        ///     is set) while the high offset is updated on each fetched message set from
        ///     the broker. If there is no cached offset (either low or high, or both) then
        ///     Offset.Invalid will be returned for the respective offset.
        /// </remarks>
        /// <param name="topicPartition">
        ///     The topic/partition of interest.
        /// </param>
        /// <returns>
        ///     The requested WatermarkOffsets (see that class for additional documentation).
        /// </returns>
        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
        {
            if (!Handle.Owner.GetType().Name.Contains("Consumer"))
            {
                throw new InvalidCastException(
                    "GetWatermarkOffsets is only available on AdminClient instances constructed from a Consumer handle.");
            }
            return kafkaHandle.GetWatermarkOffsets(topicPartition.Topic, topicPartition.Partition);
        }

        /// <summary>
        ///     Query the Kafka cluster for low (oldest available/beginning) and high (newest/end)
        ///     offsets for the specified topic/partition (blocking).
        /// </summary>
        /// <param name="topicPartition">
        ///     The topic/partition of interest.
        /// </param>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
        /// </param>
        /// <returns>
        ///     The requested WatermarkOffsets (see that class for additional documentation).
        /// </returns>
        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
            => kafkaHandle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     Query the Kafka cluster for low (oldest available/beginning) and high (newest/end)
        ///     offsets for the specified topic/partition (blocks, potentially indefinitely).
        /// </summary>
        /// <param name="topicPartition">
        ///     The topic/partition of interest.
        /// </param>
        /// <param name="cancellationToken">
        ///     
        /// </param>
        /// <returns>
        ///     The requested WatermarkOffsets (see that class for additional documentation).
        /// </returns>
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

        /// <summary>
        ///     Adds one or more brokers to the Client's list of initial
        ///     bootstrap brokers. 
        ///
        ///     Note: Additional brokers are discovered automatically as
        ///     soon as the Client connects to any broker by querying the
        ///     broker metadata. Calling this method is only required in
        ///     some scenarios where the address of all brokers in the
        ///     cluster changes.
        /// </summary>
        /// <param name="brokers">
        ///     Comma-separated list of brokers in the same format as 
        ///     the bootstrap.server configuration parameter.
        /// </param>
        /// <remarks>
        ///     There is currently no API to remove existing configured, 
        ///     added or learnt brokers.
        /// </remarks>
        /// <returns>
        ///     The number of brokers added. This value includes brokers
        ///     that may have been specified a second time.
        /// </returns>
        public int AddBrokers(string brokers)
            => kafkaHandle.AddBrokers(brokers);

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

            DisposeResources();
        }

        private void DisposeResources()
        {
            kafkaHandle.DestroyQueue(resultQueue);

            foreach (var kv in topicHandles)
            {
                kv.Value.Dispose();
            }

            if (handle.Owner == this)
            {
                ownedClient.Dispose();
                ownedClient.Handle.LibrdkafkaHandle.FlagAsClosed();
            }
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
