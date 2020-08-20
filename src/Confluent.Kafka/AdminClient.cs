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
    internal class AdminClient : IAdminClient
    {
        private int cancellationDelayMaxMs;

        private Task callbackTask;
        private CancellationTokenSource callbackCts;

        private IntPtr resultQueue = IntPtr.Zero;

        private List<CreateTopicReport> extractTopicResults(IntPtr topicResultsPtr, int topicResultsCount)
        {
            IntPtr[] topicResultsPtrArr = new IntPtr[topicResultsCount];
            Marshal.Copy(topicResultsPtr, topicResultsPtrArr, 0, topicResultsCount);

            return topicResultsPtrArr.Select(topicResultPtr => new CreateTopicReport 
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

        private List<DescribeConfigsReport> extractResultConfigs(IntPtr configResourcesPtr, int configResourceCount)
        {
            var result = new List<DescribeConfigsReport>();

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

                result.Add(new DescribeConfigsReport { 
                    ConfigResource = new ConfigResource { Name = resourceName, Type = resourceConfigType },
                    Entries = configEntries,
                    Error = new Error(errorCode, errorReason)
                });
            }

            return result;
        }

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
                                var eventPtr = kafkaHandle.QueuePoll(resultQueue, this.cancellationDelayMaxMs);
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
                                    throw new InvalidOperationException($"Completion source type mismatch. Expected {expectedType.Name}, got {type}");
                                }

                                var errorCode = Librdkafka.event_error(eventPtr);
                                var errorStr = Librdkafka.event_error_string(eventPtr);

                                switch (type)
                                {
                                    case Librdkafka.EventType.CreateTopics_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<List<CreateTopicReport>>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                return;
                                            }

                                            var result = extractTopicResults(
                                                Librdkafka.CreateTopics_result_topics(eventPtr, out UIntPtr resultCountPtr), (int)resultCountPtr);

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<List<CreateTopicReport>>)adminClientResult).TrySetException(
                                                        new CreateTopicsException(result)));
                                            }
                                            else
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<List<CreateTopicReport>>)adminClientResult).TrySetResult(result));
                                            }
                                        }
                                        break;

                                    case Librdkafka.EventType.DeleteTopics_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<List<DeleteTopicReport>>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                return;
                                            }

                                            var result = extractTopicResults(
                                                Librdkafka.DeleteTopics_result_topics(eventPtr, out UIntPtr resultCountPtr), (int)resultCountPtr)
                                                    .Select(r => new DeleteTopicReport { Topic = r.Topic, Error = r.Error }).ToList();

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<List<DeleteTopicReport>>)adminClientResult).TrySetException(
                                                        new DeleteTopicsException(result)));
                                            }
                                            else
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<List<DeleteTopicReport>>)adminClientResult).TrySetResult(result));
                                            }
                                        }
                                        break;

                                    case Librdkafka.EventType.CreatePartitions_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<List<CreatePartitionsReport>>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                return;
                                            }

                                            var result = extractTopicResults(
                                                    Librdkafka.CreatePartitions_result_topics(eventPtr, out UIntPtr resultCountPtr), (int)resultCountPtr)
                                                        .Select(r => new CreatePartitionsReport { Topic = r.Topic, Error = r.Error }).ToList();

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<List<CreatePartitionsReport>>)adminClientResult).TrySetException(
                                                        new CreatePartitionsException(result)));
                                            }
                                            else
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<List<CreatePartitionsReport>>)adminClientResult).TrySetResult(result));
                                            }
                                        }
                                        break;

                                    case Librdkafka.EventType.DescribeConfigs_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<List<DescribeConfigsResult>>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                return;
                                            }

                                            var result = extractResultConfigs(
                                                Librdkafka.DescribeConfigs_result_resources(eventPtr, out UIntPtr cntp), (int)cntp);

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<List<DescribeConfigsResult>>)adminClientResult).TrySetException(
                                                        new DescribeConfigsException(result)));
                                            }
                                            else
                                            {
                                                var nr = result.Select(a => new DescribeConfigsResult { ConfigResource = a.ConfigResource, Entries = a.Entries }).ToList();
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<List<DescribeConfigsResult>>)adminClientResult).TrySetResult(nr));
                                            }
                                        }
                                        break;

                                    case Librdkafka.EventType.AlterConfigs_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<List<AlterConfigsReport>>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                return;
                                            }

                                            var result = extractResultConfigs(
                                                Librdkafka.AlterConfigs_result_resources(eventPtr, out UIntPtr cntp), (int)cntp)
                                                    .Select(r => new AlterConfigsReport { ConfigResource = r.ConfigResource, Error = r.Error }).ToList();

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<List<AlterConfigsReport>>)adminClientResult).TrySetException(
                                                        new AlterConfigsException(result)));
                                            }
                                            else
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<List<AlterConfigsReport>>) adminClientResult).TrySetResult(result));
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
            { Librdkafka.EventType.CreateTopics_Result, typeof(TaskCompletionSource<List<CreateTopicReport>>) },
            { Librdkafka.EventType.DeleteTopics_Result, typeof(TaskCompletionSource<List<DeleteTopicReport>>) },
            { Librdkafka.EventType.DescribeConfigs_Result, typeof(TaskCompletionSource<List<DescribeConfigsResult>>) },
            { Librdkafka.EventType.AlterConfigs_Result, typeof(TaskCompletionSource<List<AlterConfigsReport>>) },
            { Librdkafka.EventType.CreatePartitions_Result, typeof(TaskCompletionSource<List<CreatePartitionsReport>>) }
        };


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.DescribeConfigsAsync(IEnumerable{ConfigResource}, DescribeConfigsOptions)" />
        /// </summary>
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
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.AlterConfigsAsync(Dictionary{ConfigResource, List{ConfigEntry}}, AlterConfigsOptions)" />
        /// </summary>
        public Task AlterConfigsAsync(Dictionary<ConfigResource, List<ConfigEntry>> configs, AlterConfigsOptions options = null)
        {
            // TODO: To support results that may complete at different times, we may also want to implement:
            // List<Task<AlterConfigResult>> AlterConfigsConcurrent(Dictionary<ConfigResource, Config> configs, AlterConfigsOptions options = null)

            var completionSource = new TaskCompletionSource<List<AlterConfigsReport>>();
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
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.CreateTopicsAsync(IEnumerable{TopicSpecification}, CreateTopicsOptions)" />
        /// </summary>
        public Task CreateTopicsAsync(IEnumerable<TopicSpecification> topics, CreateTopicsOptions options = null)
        {
            // TODO: To support results that may complete at different times, we may also want to implement:
            // public List<Task<CreateTopicResult>> CreateTopicsConcurrent(IEnumerable<TopicSpecification> topics, CreateTopicsOptions options = null)

            var completionSource = new TaskCompletionSource<List<CreateTopicReport>>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.CreateTopics(
                topics, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.DeleteTopicsAsync(IEnumerable{string}, DeleteTopicsOptions)" />
        /// </summary>
        public Task DeleteTopicsAsync(IEnumerable<string> topics, DeleteTopicsOptions options = null)
        {
            // TODO: To support results that may complete at different times, we may also want to implement:
            // List<Task<DeleteTopicResult>> DeleteTopicsConcurrent(IEnumerable<string> topics, DeleteTopicsOptions options = null)

            var completionSource = new TaskCompletionSource<List<DeleteTopicReport>>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.DeleteTopics(
                topics, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.CreatePartitionsAsync(IEnumerable{PartitionsSpecification}, CreatePartitionsOptions)" />
        /// </summary>
        public Task CreatePartitionsAsync(
            IEnumerable<PartitionsSpecification> partitionsSpecifications, CreatePartitionsOptions options = null)
        {
            // TODO: To support results that may complete at different times, we may also want to implement:
            // List<Task<CreatePartitionResult>> CreatePartitionsConcurrent(IEnumerable<PartitionsSpecification> partitionsSpecifications, CreatePartitionsOptions options = null)

            var completionSource = new TaskCompletionSource<List<CreatePartitionsReport>>();
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
        /// <param name="handle">
        ///     An underlying librdkafka client handle that the AdminClient will use to 
        ///     make broker requests. It is valid to provide either a Consumer, Producer
        ///     or AdminClient handle.
        /// </param>
        internal AdminClient(Handle handle)
        {                            
            this.ownedClient = null;
            this.handle = handle;
            Init();
        }

        internal AdminClient(AdminClientBuilder builder)
        {
            var config = Config.ExtractCancellationDelayMaxMs(builder.Config, out this.cancellationDelayMaxMs);

            if (config.Where(prop => prop.Key.StartsWith("dotnet.producer.")).Count() > 0 ||
                config.Where(prop => prop.Key.StartsWith("dotnet.consumer.")).Count() > 0)
            {
                throw new ArgumentException("AdminClient configuration must not include producer or consumer specific configuration properties.");
            }

            // build a producer instance to use as the underlying client.
            var producerBuilder = new ProducerBuilder<Null, Null>(config);
            if (builder.LogHandler != null) { producerBuilder.SetLogHandler((_, logMessage) => builder.LogHandler(this, logMessage)); }
            if (builder.ErrorHandler != null) { producerBuilder.SetErrorHandler((_, error) => builder.ErrorHandler(this, error)); }
            if (builder.StatisticsHandler != null) { producerBuilder.SetStatisticsHandler((_, stats) => builder.StatisticsHandler(this, stats)); }
            this.ownedClient = producerBuilder.Build();
            
            this.handle = new Handle
            { 
                Owner = this,
                LibrdkafkaHandle = ownedClient.Handle.LibrdkafkaHandle
            };

            Init();
        }

        private void Init()
        {
            resultQueue = kafkaHandle.CreateQueue();

            callbackCts = new CancellationTokenSource();
            callbackTask = StartPollTask(callbackCts.Token);
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.ListGroups(TimeSpan)" />
        /// </summary>
        public List<GroupInfo> ListGroups(TimeSpan timeout)
            => kafkaHandle.ListGroups(timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.ListGroup(string, TimeSpan)" />
        /// </summary>
        public GroupInfo ListGroup(string group, TimeSpan timeout)
            => kafkaHandle.ListGroup(group, timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.GetMetadata(TimeSpan)" />
        /// </summary>
        public Metadata GetMetadata(TimeSpan timeout)
            => kafkaHandle.GetMetadata(true, null, timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.GetMetadata(string, TimeSpan)" />
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
