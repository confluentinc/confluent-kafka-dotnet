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

        private List<CreateTopicExceptionResult> extractTopicResults(IntPtr topicResultsPtr, int topicResultsCount)
        {
            IntPtr[] topicResultsPtrArr = new IntPtr[topicResultsCount];
            Marshal.Copy(topicResultsPtr, topicResultsPtrArr, 0, topicResultsCount);

            return topicResultsPtrArr.Select(topicResultPtr => new CreateTopicExceptionResult 
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

        private List<DescribeConfigsExceptionResult> extractResultConfigs(IntPtr configResourcesPtr, int configResourceCount)
        {
            var result = new List<DescribeConfigsExceptionResult>();

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

                result.Add(new DescribeConfigsExceptionResult { 
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
                                                ((TaskCompletionSource<List<CreateTopicExceptionResult>>)adminClientResult).TrySetException(new KafkaException(new Error(errorCode, errorStr)));
                                                return;
                                            }

                                            var result = extractTopicResults(
                                                Librdkafka.CreateTopics_result_topics(eventPtr, out UIntPtr resultCountPtr), (int)resultCountPtr);

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                ((TaskCompletionSource<List<CreateTopicExceptionResult>>)adminClientResult).TrySetException(new CreateTopicsException(result));
                                            }
                                            else
                                            {
                                                ((TaskCompletionSource<List<CreateTopicExceptionResult>>)adminClientResult).TrySetResult(result);
                                            }
                                        }
                                        break;

                                    case Librdkafka.EventType.DeleteTopics_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                ((TaskCompletionSource<List<DeleteTopicExceptionResult>>)adminClientResult).TrySetException(new KafkaException(new Error(errorCode, errorStr)));
                                                return;
                                            }

                                            var result = extractTopicResults(
                                                Librdkafka.DeleteTopics_result_topics(eventPtr, out UIntPtr resultCountPtr), (int)resultCountPtr)
                                                    .Select(r => new DeleteTopicExceptionResult { Topic = r.Topic, Error = r.Error }).ToList();

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                ((TaskCompletionSource<List<DeleteTopicExceptionResult>>)adminClientResult).TrySetException(new DeleteTopicsException(result));
                                            }
                                            else
                                            {
                                                ((TaskCompletionSource<List<DeleteTopicExceptionResult>>)adminClientResult).TrySetResult(result);
                                            }
                                        }
                                        break;

                                    case Librdkafka.EventType.CreatePartitions_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                ((TaskCompletionSource<List<CreatePartitionsExceptionResult>>)adminClientResult).TrySetException(new KafkaException(new Error(errorCode, errorStr)));
                                                return;
                                            }

                                            var result = extractTopicResults(
                                                    Librdkafka.CreatePartitions_result_topics(eventPtr, out UIntPtr resultCountPtr), (int)resultCountPtr)
                                                        .Select(r => new CreatePartitionsExceptionResult { Topic = r.Topic, Error = r.Error }).ToList();

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                ((TaskCompletionSource<List<CreatePartitionsExceptionResult>>)adminClientResult).TrySetException(new CreatePartitionsException(result));
                                            }
                                            else
                                            {
                                                ((TaskCompletionSource<List<CreatePartitionsExceptionResult>>)adminClientResult).TrySetResult(result);
                                            }
                                        }
                                        break;

                                    case Librdkafka.EventType.DescribeConfigs_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                ((TaskCompletionSource<List<DescribeConfigsResult>>)adminClientResult).TrySetException(new KafkaException(new Error(errorCode, errorStr)));
                                                return;
                                            }

                                            var result = extractResultConfigs(
                                                Librdkafka.DescribeConfigs_result_resources(eventPtr, out UIntPtr cntp), (int)cntp);

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                ((TaskCompletionSource<List<DescribeConfigsResult>>)adminClientResult).TrySetException(new DescribeConfigsException(result));
                                            }
                                            else
                                            {
                                                var nr = result.Select(a => new DescribeConfigsResult { ConfigResource = a.ConfigResource, Entries = a.Entries }).ToList();
                                                ((TaskCompletionSource<List<DescribeConfigsResult>>)adminClientResult).TrySetResult(nr);
                                            }
                                        }
                                        break;

                                    case Librdkafka.EventType.AlterConfigs_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                ((TaskCompletionSource<List<AlterConfigsExceptionResult>>)adminClientResult).TrySetException(new KafkaException(new Error(errorCode, errorStr)));
                                                return;
                                            }

                                            var result = extractResultConfigs(
                                                Librdkafka.AlterConfigs_result_resources(eventPtr, out UIntPtr cntp), (int)cntp)
                                                    .Select(r => new AlterConfigsExceptionResult { ConfigResource = r.ConfigResource, Error = r.Error }).ToList();

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                ((TaskCompletionSource<List<AlterConfigsExceptionResult>>)adminClientResult).TrySetException(new AlterConfigsException(result));
                                            }
                                            else
                                            {
                                                ((TaskCompletionSource<List<AlterConfigsExceptionResult>>) adminClientResult).TrySetResult(result);
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
                                break;
                            }
                        }
                    }
                    catch (OperationCanceledException) {}
                }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);


        internal static Dictionary<Librdkafka.EventType, Type> adminClientResultTypes = new Dictionary<Librdkafka.EventType, Type>
        {
            { Librdkafka.EventType.CreateTopics_Result, typeof(TaskCompletionSource<List<CreateTopicExceptionResult>>) },
            { Librdkafka.EventType.DeleteTopics_Result, typeof(TaskCompletionSource<List<DeleteTopicExceptionResult>>) },
            { Librdkafka.EventType.DescribeConfigs_Result, typeof(TaskCompletionSource<List<DescribeConfigsResult>>) },
            { Librdkafka.EventType.AlterConfigs_Result, typeof(TaskCompletionSource<List<AlterConfigsExceptionResult>>) },
            { Librdkafka.EventType.CreatePartitions_Result, typeof(TaskCompletionSource<List<CreatePartitionsExceptionResult>>) }
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
        public Task<List<DescribeConfigsResult>> DescribeConfigsAsync(IEnumerable<ConfigResource> resources, DescribeConfigsOptions options = null)
        {
            // TODO: To support results that may complete at different times, we may also want to implement:
            // List<Task<DescribeConfigResult>> DescribeConfigsConcurrent(IEnumerable<ConfigResource> resources, DescribeConfigsOptions options = null)

            var completionSource = new TaskCompletionSource<List<DescribeConfigsResult>>();
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
        ///     with version 0.11.0.0 or higher. IMPORTANT NOTE: Unspecified configuration properties
        ///     will be reverted to their default values. Furthermore, if you use DescribeConfigsAsync
        ///     to obtain the current set of configuration values, modify them, then use 
        ///     AlterConfigsAsync to set them, you will loose any non-default values that are marked 
        ///     as sensitive because they are not provided by DescribeConfigsAsync.
        /// </summary>
        /// <param name="configs">
        ///     The resources with their configs (topic is the only resource type with configs
        ///     that can be updated currently).
        /// </param>
        /// <param name="options">
        ///     The options to use when altering configs.
        /// </param>
        /// <returns>
        ///     The results of the alter configs requests.
        /// </returns>
        public Task AlterConfigsAsync(Dictionary<ConfigResource, List<ConfigEntry>> configs, AlterConfigsOptions options = null)
        {
            // TODO: To support results that may complete at different times, we may also want to implement:
            // List<Task<AlterConfigResult>> AlterConfigsConcurrent(Dictionary<ConfigResource, Config> configs, AlterConfigsOptions options = null)

            var completionSource = new TaskCompletionSource<List<AlterConfigsExceptionResult>>();
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
        ///     Create a set of new topics.
        /// </summary>
        /// <param name="topics">
        ///     A collection of specifications for the new topics to create.
        /// </param>
        /// <param name="options">
        ///     The options to use when creating the topics.
        /// </param>
        /// <returns>
        ///     The results of the create topic requests.
        /// </returns>
        public Task CreateTopicsAsync(IEnumerable<TopicSpecification> topics, CreateTopicsOptions options = null)
        {
            // TODO: To support results that may complete at different times, we may also want to implement:
            // public List<Task<CreateTopicResult>> CreateTopicsConcurrent(IEnumerable<TopicSpecification> topics, CreateTopicsOptions options = null)

            var completionSource = new TaskCompletionSource<List<CreateTopicExceptionResult>>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.CreateTopics(
                topics, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Delete a set of topics. This operation is not transactional so it may succeed for some topics while fail
        ///     for others. It may take several seconds after the DeleteTopicsResult returns success for all the brokers to
        ///     become aware that the topics are gone. During this time, topics may continue to be visible via admin 
        ///     operations. If delete.topic.enable is false on the brokers, DeleteTopicsAsync will mark the topics for
        ///     deletion, but not actually delete them. The Task will return successfully in this case.
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
        public Task DeleteTopicsAsync(IEnumerable<string> topics, DeleteTopicsOptions options = null)
        {
            // TODO: To support results that may complete at different times, we may also want to implement:
            // List<Task<DeleteTopicResult>> DeleteTopicsConcurrent(IEnumerable<string> topics, DeleteTopicsOptions options = null)

            var completionSource = new TaskCompletionSource<List<DeleteTopicExceptionResult>>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.DeleteTopics(
                topics, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Increase the number of partitions for one or more topics as per
        ///     the supplied PartitionsSpecifications.
        /// </summary>
        /// <param name="partitionsSpecifications">
        ///     A collection of PartitionsSpecifications.
        /// </param>
        /// <param name="options">
        ///     The options to use when creating the partitions.
        /// </param>
        /// <returns>
        ///     The results of the PartitionsSpecification requests.
        /// </returns>
        public Task CreatePartitionsAsync(
            IEnumerable<PartitionsSpecification> partitionsSpecifications, CreatePartitionsOptions options = null)
        {
            // TODO: To support results that may complete at different times, we may also want to implement:
            // List<Task<CreatePartitionResult>> CreatePartitionsConcurrent(IEnumerable<PartitionsSpecification> partitionsSpecifications, CreatePartitionsOptions options = null)

            var completionSource = new TaskCompletionSource<List<CreatePartitionsExceptionResult>>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.CreatePartitions(
                partitionsSpecifications, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        private IClient ownedClient;
        private Handle handle;

        private SafeKafkaHandle kafkaHandle
            => handle.LibrdkafkaHandle;


        /// <summary>
        ///     Initialize a new AdminClient instance.
        /// </summary>
        /// <param name="config">
        ///     A collection of librdkafka configuration parameters 
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     and parameters specific to this client (refer to: 
        ///     <see cref="Confluent.Kafka.ConfigPropertyNames" />). Only
        ///     the bootstrap.servers property is required.
        /// </param>
        public AdminClient(AdminClientConfig config)
        {
            if (
                config.Where(prop => prop.Key.StartsWith("dotnet.producer.")).Count() > 0 ||
                config.Where(prop => prop.Key.StartsWith("dotnet.consumer.")).Count() > 0)
            {
                throw new ArgumentException("AdminClient configuration must not include producer or consumer specific configuration properties.");
            }

            this.ownedClient = new Producer(new ProducerConfig(config));
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
            resultQueue = kafkaHandle.CreateQueue();

            callbackCts = new CancellationTokenSource();
            callbackTask = StartPollTask(callbackCts.Token);
        }


        /// <summary>
        ///     Get information pertaining to all groups in the Kafka cluster (blocking)
        ///
        ///     [API-SUBJECT-TO-CHANGE] - The API associated with this functionality 
        ///     is subject to change.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
        /// </param>
        public List<GroupInfo> ListGroups(TimeSpan timeout)
            => kafkaHandle.ListGroups(timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     Get information pertaining to a particular group in the
        ///     Kafka cluster (blocking).
        ///
        ///     [API-SUBJECT-TO-CHANGE] - The API associated with this functionality is subject to change.
        /// </summary>
        /// <param name="group">
        ///     The group of interest.
        /// </param>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
        /// </param>
        /// <returns>
        ///     Returns information pertaining to the specified group
        ///     or null if this group does not exist.
        /// </returns>
        public GroupInfo ListGroup(string group, TimeSpan timeout)
            => kafkaHandle.ListGroup(group, timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     Get the last cached low (oldest available/beginning) and high (newest/end)
        ///     offsets for a topic/partition.
        /// </summary>
        /// <remarks>
        ///     This method is only available on instances constructed from a Consumer
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
        ///     Query the cluster for metadata.
        ///
        ///     [API-SUBJECT-TO-CHANGE] - The API associated with this functionality is subject to change.
        /// </summary>
        public Metadata GetMetadata(TimeSpan timeout)
            => kafkaHandle.GetMetadata(true, null, timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     Query the cluster for metadata for a specific topic.
        /// 
        ///     [API-SUBJECT-TO-CHANGE] - The API associated with this functionality is subject to change.
        /// </summary>
        public Metadata GetMetadata(string topic, TimeSpan timeout)
            => kafkaHandle.GetMetadata(false, kafkaHandle.getKafkaTopicHandle(topic), timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IClient.AddBrokers(string)" />
        /// </summary>
        public int AddBrokers(string brokers)
            => kafkaHandle.AddBrokers(brokers);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IClient.Name" />
        /// </summary>
        public string Name
            => kafkaHandle.Name;


        /// <summary>
        ///     An opaque reference to the underlying librdkafka 
        ///     client instance.
        /// </summary>
        public Handle Handle
            => handle;


        /// <summary>
        ///     Releases all resources used by this AdminClient. In the current
        ///     implementation, this method may block for up to 100ms. This 
        ///     will be replaced with a non-blocking version in the future.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        /// <summary>
        ///     Releases the unmanaged resources used by the
        ///     <see cref="Confluent.Kafka.AdminClient" />
        ///     and optionally disposes the managed resources.
        /// </summary>
        /// <param name="disposing">
        ///     true to release both managed and unmanaged resources;
        ///     false to release only unmanaged resources.
        /// </param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
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
                        // program execution should never get here.
                        throw e.InnerException;
                    }
                }
                finally
                {
                    callbackCts.Dispose();
                }

                DisposeResources();
            }
        }


        private void DisposeResources()
        {
            kafkaHandle.DestroyQueue(resultQueue);

            if (handle.Owner == this)
            {
                ownedClient.Dispose();
            }
        }

    }
}
