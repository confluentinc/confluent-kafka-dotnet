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
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Implements a high-level Apache Kafka consumer with
    ///     deserialization capability.
    /// </summary>
    internal class Consumer<TKey, TValue> : IConsumer<TKey, TValue>, IClient
    {
        internal class Config
        {
            internal IEnumerable<KeyValuePair<string, string>> config;
            internal Action<Error> errorHandler;
            internal Action<LogMessage> logHandler;
            internal Action<string> statisticsHandler;
            internal Action<CommittedOffsets> offsetsCommittedHandler;
            internal Action<string> oAuthBearerTokenRefreshHandler;
            internal Func<List<TopicPartition>, IEnumerable<TopicPartitionOffset>> partitionsAssignedHandler;
            internal Func<List<TopicPartitionOffset>, IEnumerable<TopicPartitionOffset>> partitionsRevokedHandler;
            internal Func<List<TopicPartitionOffset>, IEnumerable<TopicPartitionOffset>> partitionsLostHandler;
            internal bool revokedOrLostHandlerIsFunc;
        }

        private IDeserializer<TKey> keyDeserializer;
        private IDeserializer<TValue> valueDeserializer;

        private Dictionary<Type, object> defaultDeserializers = new Dictionary<Type, object>
        {
            { typeof(Null), Deserializers.Null },
            { typeof(Ignore), Deserializers.Ignore },
            { typeof(int), Deserializers.Int32 },
            { typeof(long), Deserializers.Int64 },
            { typeof(string), Deserializers.Utf8 },
            { typeof(float), Deserializers.Single },
            { typeof(double), Deserializers.Double },
            { typeof(byte[]), Deserializers.ByteArray }
        };

        private int cancellationDelayMaxMs;

        private bool disposeHasBeenCalled = false;
        private object disposeHasBeenCalledLockObj = new object();

        /// <summary>
        ///     keeps track of whether or not assign has been called during
        ///     invocation of a rebalance callback event.
        /// </summary>
        private int assignCallCount = 0;
        private object assignCallCountLockObj = new object();

        private bool enableHeaderMarshaling = true;
        private bool enableTimestampMarshaling = true;
        private bool enableTopicNameMarshaling = true;

        private SafeKafkaHandle kafkaHandle;

        // .NET Exceptions are not propagated through native code, so we need to
        // do this book keeping explicitly.
        private Exception handlerException = null;

        private Action<Error> errorHandler;
        private Librdkafka.ErrorDelegate errorCallbackDelegate;
        private void ErrorCallback(IntPtr rk, ErrorCode err, string reason, IntPtr opaque)
        {
            if (kafkaHandle.IsClosed) { return; }
            try
            {
                // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
                errorHandler?.Invoke(kafkaHandle.CreatePossiblyFatalError(err, reason));
            }
            catch (Exception)
            {
                // Eat any exception thrown by user error handler code. Although these could be 
                // exposed to the application via the initiating function call easily enough,
                // they aren't for consistency with the producer (where the poll method is called)
                // on a background thread.
            }
        }

        private Action<string> statisticsHandler;
        private Librdkafka.StatsDelegate statisticsCallbackDelegate;
        private int StatisticsCallback(IntPtr rk, IntPtr json, UIntPtr json_len, IntPtr opaque)
        {
            if (kafkaHandle.IsClosed) { return 0; }
            try
            {
                // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
                statisticsHandler?.Invoke(Util.Marshal.PtrToStringUTF8(json, json_len));
            }
            catch (Exception e)
            {
                handlerException = e;
            }
            
            return 0; // instruct librdkafka to immediately free the json ptr.
        }

        private Action<string> oAuthBearerTokenRefreshHandler;
        private Librdkafka.OAuthBearerTokenRefreshDelegate oAuthBearerTokenRefreshCallbackDelegate;
        private void OAuthBearerTokenRefreshCallback(IntPtr rk, IntPtr oauthbearer_config, IntPtr opaque)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (kafkaHandle.IsClosed) { return; }

            try
            {
                oAuthBearerTokenRefreshHandler?.Invoke(Util.Marshal.PtrToStringUTF8(oauthbearer_config));
            }
            catch (Exception e)
            {
                handlerException = e;
            }
        }

        private Action<LogMessage> logHandler;
        private object loggerLockObj = new object();
        private Librdkafka.LogDelegate logCallbackDelegate;
        private void LogCallback(IntPtr rk, SyslogLevel level, string fac, string buf)
        {
            if (kafkaHandle != null && kafkaHandle.IsClosed) { return; }
            try
            {
                // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
                // Note: kafkaHandle can be null if the callback is during construction (in that case the delegate should be called).
                logHandler?.Invoke(new LogMessage(Util.Marshal.PtrToStringUTF8(Librdkafka.name(rk)), level, fac, buf));
            }
            catch (Exception)
            {
                // Eat any exception thrown by user log handler code.
            }
        }

        private Func<List<TopicPartition>, IEnumerable<TopicPartitionOffset>> partitionsAssignedHandler;
        private Func<List<TopicPartitionOffset>, IEnumerable<TopicPartitionOffset>> partitionsRevokedHandler;
        private Func<List<TopicPartitionOffset>, IEnumerable<TopicPartitionOffset>> partitionsLostHandler;
        private bool revokedOrLostHandlerIsFunc;
        private Librdkafka.RebalanceDelegate rebalanceDelegate;
        private void RebalanceCallback(
            IntPtr rk,
            ErrorCode err,
            IntPtr partitionsPtr,
            IntPtr opaque)
        {
            try
            {
                // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
                if (kafkaHandle.IsClosed)
                { 
                    // The RebalanceCallback should never be invoked as a side effect of Dispose.
                    // If for some reason flow of execution gets here, something is badly wrong. 
                    // (and we have a closed librdkafka handle that is expecting an assign call...)
                    throw new Exception("Unexpected rebalance callback on disposed kafkaHandle");
                }

                if (kafkaHandle.RebalanceProtocol == "COOPERATIVE" &&
                    this.revokedOrLostHandlerIsFunc)
                {
                    throw new InvalidOperationException("Neither revoked nor lost partition handlers may return an updated assignment when a COOPERATIVE assignor is in use");
                }

                var partitions = SafeKafkaHandle.GetTopicPartitionOffsetErrorList(partitionsPtr).Select(p => p.TopicPartition).ToList();

                if (err == ErrorCode.Local_AssignPartitions)
                {
                    if (partitionsAssignedHandler == null)
                    {
                        if (kafkaHandle.RebalanceProtocol == "COOPERATIVE")
                        {
                            IncrementalAssign(partitions.Select(p => new TopicPartitionOffset(p, Offset.Unset)));
                        }
                        else
                        {
                            Assign(partitions.Select(p => new TopicPartitionOffset(p, Offset.Unset)));
                        }
                        return;
                    }

                    lock (assignCallCountLockObj) { assignCallCount = 0; }
                    var assignTo = partitionsAssignedHandler(partitions);
                    lock (assignCallCountLockObj)
                    {
                        if (assignCallCount > 0)
                        {
                            throw new InvalidOperationException("(Incremental)Assign/Unassign must not be called in the partitions assigned handler");
                        }
                    }

                    if (kafkaHandle.RebalanceProtocol == "COOPERATIVE")
                    {
                        if (assignTo.Count() != partitions.Count())
                        {
                            throw new InvalidOperationException("The partitions assigned handler must not return a different set of topic partitions than it was provided");
                        }

                        var sortedPartitions = partitions.OrderBy(p => p).ToList();
                        var sortedAssignTo = assignTo.OrderBy(p => p.TopicPartition);

                        var partitionsIter = sortedPartitions.GetEnumerator();
                        foreach (var p in sortedAssignTo)
                        {
                            partitionsIter.MoveNext();
                            if (p.TopicPartition != partitionsIter.Current)
                            {
                                throw new InvalidOperationException("The partitions assigned handler must not return a different set of topic partitions than it was provided");
                            }
                        }

                        IncrementalAssign(sortedAssignTo);
                    }
                    else
                    {
                        Assign(assignTo);
                    }
                    return;
                }


                if (err == ErrorCode.Local_RevokePartitions)
                {
                    if (partitionsRevokedHandler == null &&
                        (!kafkaHandle.AssignmentLost || partitionsLostHandler == null))
                    {
                        if (kafkaHandle.RebalanceProtocol == "COOPERATIVE")
                        {
                            IncrementalUnassign(partitions);
                        }
                        else
                        {
                            Unassign();
                        }
                        return;
                    }

                    var assignmentWithPositions = new List<TopicPartitionOffset>();
                    foreach (var tp in partitions)
                    {
                        try
                        {
                            assignmentWithPositions.Add(this.PositionTopicPartitionOffset(tp));
                        }
                        catch
                        {
                            assignmentWithPositions.Add(new TopicPartitionOffset(tp, Offset.Unset));
                        }
                    }

                    lock (assignCallCountLockObj) { assignCallCount = 0; }
                    var assignTo = kafkaHandle.AssignmentLost
                        ? (partitionsLostHandler != null
                            ? partitionsLostHandler(assignmentWithPositions)
                            : partitionsRevokedHandler(assignmentWithPositions))
                        : partitionsRevokedHandler(assignmentWithPositions);
                    lock (assignCallCountLockObj)
                    {
                        if (assignCallCount > 0)
                        {
                            throw new InvalidOperationException("Assign/Unassign must not be called in the partitions revoked handler");
                        }
                    }

                    if (kafkaHandle.RebalanceProtocol == "COOPERATIVE")
                    {
                        // assignTo is always empty, not used in the COOPERATIVE case.
                        IncrementalUnassign(partitions);
                    }
                    else
                    {
                        Unassign();
                    }
                    return;
                }

                throw new KafkaException(kafkaHandle.CreatePossiblyFatalError(err, null));
            }
            catch (Exception e)
            {
                Unassign();
                handlerException = e;
            }
        }

        private Action<CommittedOffsets> offsetsCommittedHandler;
        private Librdkafka.CommitDelegate commitDelegate;
        private void CommitCallback(
            IntPtr rk,
            ErrorCode err,
            IntPtr offsets,
            IntPtr opaque)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (kafkaHandle.IsClosed) { return; }

            try
            {
                offsetsCommittedHandler?.Invoke(new CommittedOffsets(
                    SafeKafkaHandle.GetTopicPartitionOffsetErrorList(offsets),
                    kafkaHandle.CreatePossiblyFatalError(err, null)
                ));
            }
            catch (Exception e)
            {
                handlerException = e;
            }
        }

        private static byte[] KeyAsByteArray(rd_kafka_message msg)
        {
            byte[] keyAsByteArray = null;
            if (msg.key != IntPtr.Zero)
            {
                keyAsByteArray = new byte[(int) msg.key_len];
                Marshal.Copy(msg.key, keyAsByteArray, 0, (int) msg.key_len);
            }
            return keyAsByteArray;
        }

        private static byte[] ValueAsByteArray(rd_kafka_message msg)
        {
            byte[] valAsByteArray = null;
            if (msg.val != IntPtr.Zero)
            {
                valAsByteArray = new byte[(int) msg.len];
                Marshal.Copy(msg.val, valAsByteArray, 0, (int) msg.len);
            }
            return valAsByteArray;
        }


        /// <inheritdoc/>
        public List<TopicPartition> Assignment
            => kafkaHandle.GetAssignment();


        /// <inheritdoc/>
        public List<string> Subscription
            => kafkaHandle.GetSubscription();


        /// <inheritdoc/>
        public void Subscribe(IEnumerable<string> topics)
        {
            kafkaHandle.Subscribe(topics);
        }


        /// <inheritdoc/>
        public void Subscribe(string topic)
            => Subscribe(new[] { topic });


        /// <inheritdoc/>
        public void Unsubscribe()
            => kafkaHandle.Unsubscribe();


        /// <inheritdoc/>
        public void Assign(TopicPartition partition)
            => Assign(new List<TopicPartition> { partition });


        /// <inheritdoc/>
        public void Assign(TopicPartitionOffset partition)
            => Assign(new List<TopicPartitionOffset> { partition });


        /// <inheritdoc/>
        public void Assign(IEnumerable<TopicPartitionOffset> partitions)
        {
            lock (assignCallCountLockObj) { assignCallCount += 1; }
            kafkaHandle.Assign(partitions.ToList());
        }


        /// <inheritdoc/>
        public void Assign(IEnumerable<TopicPartition> partitions)
        {
            lock (assignCallCountLockObj) { assignCallCount += 1; }
            kafkaHandle.Assign(partitions.Select(p => new TopicPartitionOffset(p, Offset.Unset)).ToList());
        }


        /// <inheritdoc/>
        public void IncrementalAssign(IEnumerable<TopicPartitionOffset> partitions)
        {
            lock (assignCallCountLockObj) { assignCallCount += 1; }
            kafkaHandle.IncrementalAssign(partitions.ToList());
        }


        /// <inheritdoc/>
        public void IncrementalAssign(IEnumerable<TopicPartition> partitions)
            => IncrementalAssign(partitions.Select(p => new TopicPartitionOffset(p, Offset.Unset)));


        /// <inheritdoc/>
        public void IncrementalUnassign(IEnumerable<TopicPartition> partitions)
        {
            lock (assignCallCountLockObj) { assignCallCount += 1; }
            kafkaHandle.IncrementalUnassign(partitions.Select(p => new TopicPartitionOffset(p, Offset.Unset)).ToList());
        }


        /// <inheritdoc/>
        public void Unassign()
        {
            lock (assignCallCountLockObj) { assignCallCount += 1; }
            kafkaHandle.Assign(null);
        }


        /// <inheritdoc/>
        public void StoreOffset(ConsumeResult<TKey, TValue> result)
            => StoreOffset(new TopicPartitionOffset(result.TopicPartition,
                    result.Offset + 1, result.LeaderEpoch));


        /// <inheritdoc/>
        public void StoreOffset(TopicPartitionOffset offset)
        {
            try
            {
                kafkaHandle.StoreOffsets(new [] { offset });
            }
            catch (TopicPartitionOffsetException e)
            {
                throw new KafkaException(e.Results[0].Error);
            }
        }


        /// <inheritdoc/>
        public List<TopicPartitionOffset> Commit()
            // TODO: use a librdkafka queue for this.
            => kafkaHandle.Commit(null);


        /// <inheritdoc/>
        public void Commit(IEnumerable<TopicPartitionOffset> offsets)
            // TODO: use a librdkafka queue for this.
            => kafkaHandle.Commit(offsets);


        /// <inheritdoc/>
        public void Commit(ConsumeResult<TKey, TValue> result)
        {
            if (result.Message == null)
            {
                throw new InvalidOperationException("Attempt was made to commit offset corresponding to an empty consume result");
            }

            Commit(new [] { new TopicPartitionOffset(result.TopicPartition, result.Offset + 1,
                                result.LeaderEpoch) });
        }


        /// <inheritdoc/>
        public void Seek(TopicPartitionOffset tpo)
            => kafkaHandle.Seek(tpo.Topic, tpo.Partition, tpo.Offset, -1);


        /// <inheritdoc/>
        public void Pause(IEnumerable<TopicPartition> partitions)
            => kafkaHandle.Pause(partitions);


        /// <inheritdoc/>
        public void Resume(IEnumerable<TopicPartition> partitions)
            => kafkaHandle.Resume(partitions);


        /// <inheritdoc/>
        public List<TopicPartitionOffset> Committed(TimeSpan timeout)
            // TODO: use a librdkafka queue for this.
            => kafkaHandle.Committed(Assignment, (IntPtr)timeout.TotalMillisecondsAsInt());


        /// <inheritdoc/>
        public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
            // TODO: use a librdkafka queue for this.
            => kafkaHandle.Committed(partitions, (IntPtr)timeout.TotalMillisecondsAsInt());


        /// <inheritdoc/>
        public Offset Position(TopicPartition partition)
        {
            try
            {
                return kafkaHandle.Position(new List<TopicPartition> { partition }).First().Offset;
            }
            catch (TopicPartitionOffsetException e)
            {
                throw new KafkaException(e.Results[0].Error);
            }
        }

        /// <inheritdoc/>
        public List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout)
            // TODO: use a librdkafka queue for this.
            => kafkaHandle.OffsetsForTimes(timestampsToSearch, timeout.TotalMillisecondsAsInt());


        /// <inheritdoc/>
        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
            => kafkaHandle.GetWatermarkOffsets(topicPartition.Topic, topicPartition.Partition);


        /// <inheritdoc/>
        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
            => kafkaHandle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, timeout.TotalMillisecondsAsInt());


        /// <inheritdoc/>
        public string MemberId
            => kafkaHandle.MemberId;


        /// <inheritdoc/>
        public int AddBrokers(string brokers)
            => kafkaHandle.AddBrokers(brokers);

        /// <inheritdoc/>
        public void SetSaslCredentials(string username, string password)
            => kafkaHandle.SetSaslCredentials(username, password);

        /// <inheritdoc/>
        public string Name
            => kafkaHandle.Name;


        /// <inheritdoc/>
        public Handle Handle
            => new Handle { Owner = this, LibrdkafkaHandle = kafkaHandle };

        /// <inheritdoc/>
        public void Close()
        {
            // commits offsets and unsubscribes.
            kafkaHandle.ConsumerClose();
            if (this.handlerException != null)
            {
                var ex = this.handlerException;
                this.handlerException = null;
                throw ex;
            }

            Dispose(true);
            GC.SuppressFinalize(this);
        }


        /// <summary>
        ///     Releases all resources used by this Consumer without
        ///     committing offsets and without alerting the group coordinator
        ///     that the consumer is exiting the group. If you do not call 
        ///     <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Close" /> or
        ///     <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Unsubscribe" />
        ///     prior to Dispose, the group will rebalance after a timeout 
        ///     specified by group's `session.timeout.ms`.
        ///     You should commit offsets / unsubscribe from the group before 
        ///     calling this method (typically by calling 
        ///     <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Close()" />).
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        /// <summary>
        ///     Releases the unmanaged resources used by the
        ///     <see cref="Confluent.Kafka.Consumer{TKey,TValue}" />
        ///     and optionally disposes the managed resources.
        /// </summary>
        /// <param name="disposing">
        ///     true to release both managed and unmanaged resources;
        ///     false to release only unmanaged resources.
        /// </param>
        protected virtual void Dispose(bool disposing)
        {
            // Calling Dispose a second or subsequent time should be a no-op.
            lock (disposeHasBeenCalledLockObj)
            { 
                if (disposeHasBeenCalled) { return; }
                disposeHasBeenCalled = true;
            }

            if (disposing)
            {
                // calls to rd_kafka_destroy may result in callbacks
                // as a side-effect. however the callbacks this class
                // registers with librdkafka ensure that any registered
                // events are not called if the kafkaHandle is closed.
                // this avoids deadlocks in common scenarios.
                kafkaHandle.Dispose();
            }
        }


        internal Consumer(ConsumerBuilder<TKey, TValue> builder)
        {
            var baseConfig = builder.ConstructBaseConfig(this);

            this.statisticsHandler = baseConfig.statisticsHandler;
            this.logHandler = baseConfig.logHandler;
            this.errorHandler = baseConfig.errorHandler;
            this.partitionsAssignedHandler = baseConfig.partitionsAssignedHandler;
            this.partitionsRevokedHandler = baseConfig.partitionsRevokedHandler;
            this.partitionsLostHandler = baseConfig.partitionsLostHandler;
            this.offsetsCommittedHandler = baseConfig.offsetsCommittedHandler;
            this.oAuthBearerTokenRefreshHandler = baseConfig.oAuthBearerTokenRefreshHandler;
            this.revokedOrLostHandlerIsFunc = baseConfig.revokedOrLostHandlerIsFunc;
            Librdkafka.Initialize(null);

            var config = Confluent.Kafka.Config.ExtractCancellationDelayMaxMs(baseConfig.config, out this.cancellationDelayMaxMs);

            if (config.FirstOrDefault(prop => string.Equals(prop.Key, "group.id", StringComparison.Ordinal)).Value == null)
            {
                throw new ArgumentException("'group.id' configuration parameter is required and was not specified.");
            }

            var modifiedConfig = Library.NameAndVersionConfig
                .Concat(config.Where(prop => prop.Key != ConfigPropertyNames.Consumer.ConsumeResultFields))
                .ToList();

            var enabledFieldsObj = config.FirstOrDefault(prop => prop.Key == ConfigPropertyNames.Consumer.ConsumeResultFields).Value;
            if (enabledFieldsObj != null)
            {
                var fields = enabledFieldsObj.Replace(" ", "");
                if (fields != "all")
                {
                    this.enableHeaderMarshaling = false;
                    this.enableTimestampMarshaling = false;
                    this.enableTopicNameMarshaling = false;
                    if (fields != "none")
                    {
                        var parts = fields.Split(',');
                        foreach (var part in parts)
                        {
                            switch (part)
                            {
                                case "headers": this.enableHeaderMarshaling = true; break;
                                case "timestamp": this.enableTimestampMarshaling = true; break;
                                case "topic": this.enableTopicNameMarshaling = true; break;
                                default: throw new ArgumentException(
                                    $"Unexpected consume result field name '{part}' in config value '{ConfigPropertyNames.Consumer.ConsumeResultFields}'.");
                            }
                        }
                    }
                }
            }

            var configHandle = SafeConfigHandle.Create();

            modifiedConfig.ForEach((kvp) =>
                {
                    if (kvp.Value == null) { throw new ArgumentNullException($"'{kvp.Key}' configuration parameter must not be null."); }
                    configHandle.Set(kvp.Key, kvp.Value);
                });

            // Explicitly keep references to delegates so they are not reclaimed by the GC.
            rebalanceDelegate = RebalanceCallback;
            commitDelegate = CommitCallback;
            errorCallbackDelegate = ErrorCallback;
            logCallbackDelegate = LogCallback;
            statisticsCallbackDelegate = StatisticsCallback;
            oAuthBearerTokenRefreshCallbackDelegate = OAuthBearerTokenRefreshCallback;

            IntPtr configPtr = configHandle.DangerousGetHandle();

            Librdkafka.conf_set_rebalance_cb(configPtr, rebalanceDelegate);

            if (offsetsCommittedHandler != null)
            {
                Librdkafka.conf_set_offset_commit_cb(configPtr, commitDelegate);
            }

            if (errorHandler != null)
            {
                Librdkafka.conf_set_error_cb(configPtr, errorCallbackDelegate);
            }
            if (logHandler != null)
            {
                Librdkafka.conf_set_log_cb(configPtr, logCallbackDelegate);
            }
            if (statisticsHandler != null)
            {
                Librdkafka.conf_set_stats_cb(configPtr, statisticsCallbackDelegate);
            }
            if (oAuthBearerTokenRefreshHandler != null)
            {
                Librdkafka.conf_set_oauthbearer_token_refresh_cb(configPtr, oAuthBearerTokenRefreshCallbackDelegate);
            }

            this.kafkaHandle = SafeKafkaHandle.Create(RdKafkaType.Consumer, configPtr, this);
            configHandle.SetHandleAsInvalid(); // config object is no longer useable.

            var pollSetConsumerError = kafkaHandle.PollSetConsumer();
            if (pollSetConsumerError != ErrorCode.NoError)
            {
                throw new KafkaException(new Error(pollSetConsumerError,
                    $"Failed to redirect the poll queue to consumer_poll queue: {ErrorCodeExtensions.GetReason(pollSetConsumerError)}"));
            }

            // setup key deserializer.
            if (builder.KeyDeserializer == null)
            {
                if (!defaultDeserializers.TryGetValue(typeof(TKey), out object deserializer))
                {
                    throw new InvalidOperationException(
                        $"Key deserializer was not specified and there is no default deserializer defined for type {typeof(TKey).Name}.");
                }
                this.keyDeserializer = (IDeserializer<TKey>)deserializer;
            }
            else
            {
                this.keyDeserializer = builder.KeyDeserializer;
            }

            // setup value deserializer.
            if (builder.ValueDeserializer == null)
            {
                if (!defaultDeserializers.TryGetValue(typeof(TValue), out object deserializer))
                {
                    throw new InvalidOperationException(
                        $"Value deserializer was not specified and there is no default deserializer defined for type {typeof(TValue).Name}.");
                }
                this.valueDeserializer = (IDeserializer<TValue>)deserializer;
            }
            else
            {
                this.valueDeserializer = builder.ValueDeserializer;
            }
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IConsumer{TKey, TValue}.Consume(int)" />
        /// </summary>
        public ConsumeResult<TKey, TValue> Consume(int millisecondsTimeout)
        {
            var msgPtr = kafkaHandle.ConsumerPoll((IntPtr)millisecondsTimeout);

            if (this.handlerException != null)
            {
                var ex = this.handlerException;
                this.handlerException = null;
                if (msgPtr != IntPtr.Zero)
                {
                    Librdkafka.message_destroy(msgPtr);
                }
                throw ex;
            }

            if (msgPtr == IntPtr.Zero)
            {
                return null;
            }

            try
            {
                var msg = Util.Marshal.PtrToStructure<rd_kafka_message>(msgPtr);
                int? msgLeaderEpoch = null;
                Offset msgOffset = msg.offset;

                if (msg.rkt != IntPtr.Zero && (msgOffset != Offset.Unset))
                {
                    msgLeaderEpoch = Librdkafka.message_leader_epoch(msgPtr);
                }

                string topic = null;
                if (this.enableTopicNameMarshaling)
                {
                    if (msg.rkt != IntPtr.Zero)
                    {
                        topic = Util.Marshal.PtrToStringUTF8(Librdkafka.topic_name(msg.rkt));
                    }
                }

                if (msg.err == ErrorCode.Local_PartitionEOF)
                {
                    return new ConsumeResult<TKey, TValue>
                    {
                        TopicPartitionOffset = new TopicPartitionOffset(topic,
                            msg.partition, msg.offset,
                            msgLeaderEpoch),
                        Message = null,
                        IsPartitionEOF = true
                    };
                }

                long timestampUnix = 0;
                IntPtr timestampType = (IntPtr)TimestampType.NotAvailable;
                if (enableTimestampMarshaling)
                {
                    timestampUnix = Librdkafka.message_timestamp(msgPtr, out timestampType);
                }
                var timestamp = new Timestamp(timestampUnix, (TimestampType)timestampType);

                Headers headers = null;
                if (enableHeaderMarshaling)
                {
                    headers = new Headers();
                    Librdkafka.message_headers(msgPtr, out IntPtr hdrsPtr);
                    if (hdrsPtr != IntPtr.Zero)
                    {
                        for (var i=0; ; ++i)
                        {
                            var err = Librdkafka.header_get_all(hdrsPtr, (IntPtr)i, out IntPtr namep, out IntPtr valuep, out IntPtr sizep);
                            if (err != ErrorCode.NoError)
                            {
                                break;
                            }
                            var headerName = Util.Marshal.PtrToStringUTF8(namep);
                            byte[] headerValue = null;
                            if (valuep != IntPtr.Zero)
                            {
                                headerValue = new byte[(int)sizep];
                                Marshal.Copy(valuep, headerValue, 0, (int)sizep);
                            }
                            headers.Add(headerName, headerValue);
                        }
                    }
                }

                if (msg.err != ErrorCode.NoError)
                {
                    throw new ConsumeException(
                        new ConsumeResult<byte[], byte[]>
                        {
                            TopicPartitionOffset = new TopicPartitionOffset(topic,
                                msg.partition, msg.offset,
                                msgLeaderEpoch),
                            Message = new Message<byte[], byte[]>
                            {
                                Timestamp = timestamp,
                                Headers = headers,
                                Key = KeyAsByteArray(msg),
                                Value = ValueAsByteArray(msg)
                            },
                            IsPartitionEOF = false
                        },
                        kafkaHandle.CreatePossiblyFatalMessageError(msgPtr));
                }

                TKey key;
                try
                {
                    unsafe
                    {
                        key = keyDeserializer.Deserialize(
                            msg.key == IntPtr.Zero
                                ? ReadOnlySpan<byte>.Empty
                                : new ReadOnlySpan<byte>(msg.key.ToPointer(), (int)msg.key_len),
                            msg.key == IntPtr.Zero,
                            new SerializationContext(MessageComponentType.Key, topic, headers));
                    }
                }
                catch (Exception ex)
                {
                    throw new ConsumeException(
                        new ConsumeResult<byte[], byte[]>
                        {
                            TopicPartitionOffset = new TopicPartitionOffset(topic,
                                msg.partition, msg.offset,
                                msgLeaderEpoch),
                            Message = new Message<byte[], byte[]>
                            {
                                Timestamp = timestamp,
                                Headers = headers,
                                Key = KeyAsByteArray(msg),
                                Value = ValueAsByteArray(msg)
                            },
                            IsPartitionEOF = false
                        },
                        new Error(ErrorCode.Local_KeyDeserialization),
                        ex);
                }

                TValue val;
                try
                {
                    unsafe
                    {
                        val = valueDeserializer.Deserialize(
                            msg.val == IntPtr.Zero
                                ? ReadOnlySpan<byte>.Empty
                                : new ReadOnlySpan<byte>(msg.val.ToPointer(), (int)msg.len),
                            msg.val == IntPtr.Zero,
                            new SerializationContext(MessageComponentType.Value, topic, headers));
                    }
                }
                catch (Exception ex)
                {
                    throw new ConsumeException(
                        new ConsumeResult<byte[], byte[]>
                        {
                            TopicPartitionOffset = new TopicPartitionOffset(topic,
                                msg.partition, msg.offset,
                                msgLeaderEpoch),
                            Message = new Message<byte[], byte[]>
                            {
                                Timestamp = timestamp,
                                Headers = headers,
                                Key = KeyAsByteArray(msg),
                                Value = ValueAsByteArray(msg)
                            },
                            IsPartitionEOF = false
                        },
                        new Error(ErrorCode.Local_ValueDeserialization),
                        ex);
                }

                return new ConsumeResult<TKey, TValue> 
                {
                    TopicPartitionOffset = new TopicPartitionOffset(topic,
                        msg.partition, msg.offset,
                        msgLeaderEpoch),
                    Message = new Message<TKey, TValue>
                    {
                        Timestamp = timestamp,
                        Headers = headers,
                        Key = key,
                        Value = val
                    },
                    IsPartitionEOF = false
                };
            }
            finally
            {
                Librdkafka.message_destroy(msgPtr);
            }
        }


        /// <inheritdoc/>
        public ConsumeResult<TKey, TValue> Consume(CancellationToken cancellationToken = default(CancellationToken))
        {
            while (true)
            {
                // Note: An alternative to throwing on cancellation is to return null,
                // but that would be problematic downstream (require null checks).
                cancellationToken.ThrowIfCancellationRequested();
                ConsumeResult<TKey, TValue> result = Consume(cancellationDelayMaxMs);
                if (result == null)
                {
                    continue;
                }
                return result;
            }
        }


        /// <inheritdoc/>
        public ConsumeResult<TKey, TValue> Consume(TimeSpan timeout)
            => Consume(timeout.TotalMillisecondsAsInt());


        /// <inheritdoc/>
        public IConsumerGroupMetadata ConsumerGroupMetadata
        {
            get
            {
                var cgmd = this.kafkaHandle.GetConsumerGroupMetadata();
                try
                {
                    return new ConsumerGroupMetadata { serializedMetadata = this.kafkaHandle.SerializeConsumerGroupMetadata(cgmd) };
                }
                finally
                {
                    this.kafkaHandle.DestroyConsumerGroupMetadata(cgmd);
                }
            }
        }
    }
}
