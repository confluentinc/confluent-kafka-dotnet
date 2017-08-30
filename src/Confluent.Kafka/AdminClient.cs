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
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Implements a high-level Apache Kafka producer (without serialization).
    /// </summary>
    public class AdminClient : IDisposable, IAdmin
    {
        private bool manualPoll;
        private bool disableDeliveryReports;
        
        private SafeKafkaHandle kafkaHandle;

        private Task callbackTask;
        private CancellationTokenSource callbackCts;

        private const int POLL_TIMEOUT_MS = 100;
        private Task StartPollTask(CancellationToken ct)
            => Task.Factory.StartNew(() =>
                {
                    while (!ct.IsCancellationRequested)
                    {
                        kafkaHandle.Poll((IntPtr)POLL_TIMEOUT_MS);
                    }
                }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        private LibRdKafka.ErrorDelegate errorDelegate;
        private void ErrorCallback(IntPtr rk, ErrorCode err, string reason, IntPtr opaque)
        {
            OnError?.Invoke(this, new Error(err, reason));
        }

        private LibRdKafka.StatsDelegate statsDelegate;
        private int StatsCallback(IntPtr rk, IntPtr json, UIntPtr json_len, IntPtr opaque)
        {
            OnStatistics?.Invoke(this, Util.Marshal.PtrToStringUTF8(json));
            return 0; // instruct librdkafka to immediately free the json ptr.
        }

        private LibRdKafka.LogDelegate logDelegate;
        private void LogCallback(IntPtr rk, int level, string fac, string buf)
        {
            var name = Util.Marshal.PtrToStringUTF8(LibRdKafka.name(rk));

            if (OnLog == null)
            {
                // Log to stderr by default if no logger is specified.
                Loggers.ConsoleLogger(this, new LogMessage(name, level, fac, buf));
                return;
            }

            OnLog.Invoke(this, new LogMessage(name, level, fac, buf));
        }

        /// <summary>
        ///     Initializes a new Producer instance.
        /// </summary>
        /// <param name="config">
        ///     librdkafka configuration parameters (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
        ///     Topic configuration parameters are specified via the "default.topic.config" sub-dictionary config parameter.
        /// </param>
        /// <param name="manualPoll">
        ///     If true, does not start a dedicated polling thread to trigger events or receive delivery reports -
        ///     you must call the Poll method periodically instead. Typically you should set this parameter to false.
        /// </param>
        public AdminClient(IEnumerable<KeyValuePair<string, object>> config, bool manualPoll)
        {
            this.manualPoll = manualPoll;

            var configHandle = SafeConfigHandle.Create();
            config
                .Where(prop => prop.Key != "default.topic.config")
                .ToList()
                .ForEach((kvp) => { configHandle.Set(kvp.Key, kvp.Value.ToString()); });

            IntPtr configPtr = configHandle.DangerousGetHandle();
            
            // Explicitly keep references to delegates so they are not reclaimed by the GC.
            errorDelegate = ErrorCallback;
            logDelegate = LogCallback;
            statsDelegate = StatsCallback;

            // TODO: provide some mechanism whereby calls to the error and log callbacks are cached until
            //       such time as event handlers have had a chance to be registered.
            LibRdKafka.conf_set_error_cb(configPtr, errorDelegate);
            LibRdKafka.conf_set_log_cb(configPtr, logDelegate);
            LibRdKafka.conf_set_stats_cb(configPtr, statsDelegate);

            this.kafkaHandle = SafeKafkaHandle.Create(RdKafkaType.Producer, configPtr);
            configHandle.SetHandleAsInvalid(); // config object is no longer useable.

            if (!manualPoll)
            {
                callbackCts = new CancellationTokenSource();
                callbackTask = StartPollTask(callbackCts.Token);
            }
        }

        /// <summary>
        ///     Initializes a new Producer instance.
        /// </summary>
        /// <param name="config">
        ///     librdkafka configuration parameters (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
        ///     Topic configuration parameters are specified via the "default.topic.config" sub-dictionary config parameter.
        /// </param>
        public AdminClient(IEnumerable<KeyValuePair<string, object>> config)
            : this(config, false) {}


        /// <summary>
        ///     Poll for callback events. You will not typically need
        ///     to call this method. Only call on producer instances 
        ///     where background polling is not enabled.
        /// </summary>
        /// <param name="millisecondsTimeout">
        ///     The maximum period of time to block (in milliseconds) if no
        ///     callback events are waiting or -1 to block indefinitely. 
        ///     You should typically use a relatively short timout period 
        ///     because this operation cannot be cancelled.
        /// </param>
        /// <returns>
        ///     Returns the number of events served.
        /// </returns>
        public int Poll(int millisecondsTimeout)
        {
            if (!manualPoll)
            {
                throw new InvalidOperationException("Poll method called when manualPoll not enabled.");
            }
            return kafkaHandle.Poll((IntPtr)millisecondsTimeout);
        }

        /// <summary>
        ///     Poll for callback events. You will not typically need
        ///     to call this method. Only call on producer instances 
        ///     where background polling is not enabled.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum period of time to block if no callback events
        ///     are waiting. You should typically use a relatively short 
        ///     timout period because this operation cannot be cancelled.
        /// </param>
        /// <returns>
        ///     Returns the number of events served.
        /// </returns>
        public int Poll(TimeSpan timeout)
            => Poll(timeout.TotalMillisecondsAsInt());
        
        /// <summary>
        ///     Raised on critical errors, e.g. connection failures or all 
        ///     brokers down. Note that the client will try to automatically 
        ///     recover from errors - these errors should be seen as 
        ///     informational rather than catastrophic
        /// </summary>
        /// <remarks>
        ///     Called on the Producer poll thread.
        /// </remarks>
        public event EventHandler<Error> OnError;

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
        public event EventHandler<string> OnStatistics;

        /// <summary>
        ///     Raised when there is information that should be logged.
        /// </summary>
        /// <remarks>
        ///     By default not many log messages are generated.
        /// 
        ///     You can specify one or more debug contexts using the 'debug'
        ///     configuration property and a log level using the 'log_level'
        ///     configuration property to enable more verbose logging,
        ///     however you shouldn't typically need to do this.
        ///
        ///     Warning: Log handlers are called spontaneously from internal librdkafka 
        ///     threads and the application must not call any Confluent.Kafka APIs from 
        ///     within a log handler or perform any prolonged operations.
        /// </remarks>
        public event EventHandler<LogMessage> OnLog;
        
        /// <summary>
        ///     Gets the name of this producer instance.
        ///     Contains (but is not equal to) the client.id configuration parameter.
        /// </summary>
        /// <remarks>
        ///     This name will be unique across all producer instances
        ///     in a given application which allows log messages to be
        ///     associated with the corresponding instance.
        /// </remarks>
        public string Name
            => kafkaHandle.Name;
        
        /// <summary>
        ///     Releases all resources used by this AdminClient.
        /// </summary>
        public void Dispose()
        {
            if (!this.manualPoll)
            {
                // Note: It's necessary to wait on callbackTask before disposing kafkaHandle.
                // TODO: If called in a finalizer, can callbackTask be potentially null?
                callbackCts.Cancel();
                callbackTask.Wait();
            }
            kafkaHandle.Dispose();
        }

        /// <summary>
        ///     Get information pertaining to all groups in the Kafka cluster (blocking)
        ///
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
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
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
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
        ///     Get information pertaining to a particular group in the
        ///     Kafka cluster (blocks, potentially indefinitely).
        ///
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        /// <param name="group">
        ///     The group of interest.
        /// </param>
        /// <returns>
        ///     Returns information pertaining to the specified group
        ///     or null if this group does not exist.
        /// </returns>
        public GroupInfo ListGroup(string group)
            => kafkaHandle.ListGroup(group, -1);


        /// <summary>
        ///     Query the Kafka cluster for low (oldest/beginning) and high (newest/end)
        ///     offsets for the specified topic/partition (blocking).
        ///
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        /// <param name="topicPartition">
        ///     The topic/partition of interest.
        /// </param>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
        /// </param>
        /// <returns>
        ///     The requested WatermarkOffsets.
        /// </returns>
        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
            => kafkaHandle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, timeout.TotalMillisecondsAsInt());

        /// <summary>
        ///     Query the Kafka cluster for low (oldest/beginning) and high (newest/end)
        ///     offsets for the specified topic/partition (blocks, potentially indefinitely).
        ///
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        /// <param name="topicPartition">
        ///     The topic/partition of interest.
        /// </param>
        /// <returns>
        ///     The requested WatermarkOffsets.
        /// </returns>
        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition)
            => kafkaHandle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, -1);

        private Metadata GetMetadata(bool allTopics, int millisecondsTimeout)
            => kafkaHandle.GetMetadata(allTopics, null, millisecondsTimeout);

        /// <summary>
        ///     Query the cluster for metadata (blocking).
        ///
        ///     - allTopics = true - request all topics from cluster
        ///     - allTopics = false, - request only locally known topics.
        ///
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        public Metadata GetMetadata(bool allTopics, TimeSpan timeout)
            => kafkaHandle.GetMetadata(allTopics, null, timeout.TotalMillisecondsAsInt());

        //TODO add Getmetadata for single topic

        /// <summary>
        ///     Refer to <see cref="GetMetadata(bool,TimeSpan)" />
        ///     
        ///     [UNSTABLE-API] - The API associated with this functionality is subject to change.
        /// </summary>
        public Metadata GetMetadata()
            => GetMetadata(true, -1);

        /// <summary>
        ///     Adds one or more brokers to the Producer's list of initial
        ///     bootstrap brokers. 
        ///
        ///     Note: Additional brokers are discovered automatically as 
        ///     soon as the Producer connects to any broker by querying the 
        ///     broker metadata. Calling this method is only required in 
        ///     some scenarios where the address of all brokers in the 
        ///     cluster changes.
        /// </summary>
        /// <param name="brokers">
        ///     Coma-separated list of brokers in the same format as 
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
        
        public List<TopicError> DeleteTopics(IEnumerable<string> topics, TimeSpan timeout, bool waitForDeletion)
            => kafkaHandle.AdminDeleteTopics(topics, timeout.TotalMillisecondsAsInt(), waitForDeletion);
    }
}
