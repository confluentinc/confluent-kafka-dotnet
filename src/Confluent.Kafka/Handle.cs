using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using System.Threading;
using Confluent.Kafka.Impl;

namespace Confluent.Kafka
{
    /// <summary>
    /// Shared base of <see cref="Consumer" /> and <see cref="Producer" />.
    /// </summary>
    public class Handle : IDisposable
    {
        internal SafeKafkaHandle handle;
        LibRdKafka.ErrorCallback ErrorDelegate;
        LibRdKafka.LogCallback LogDelegate;
        LibRdKafka.StatsCallback StatsDelegate;
        Task callbackTask;
        CancellationTokenSource callbackCts;

        ~Handle()
        {
            Dispose(false);
        }

        internal void Init(RdKafkaType type, IntPtr config, Config.LogCallback logger)
        {
            ErrorDelegate = (IntPtr rk, ErrorCode err, string reason, IntPtr opaque) =>
            {
                OnError?.Invoke(this, new ErrorArgs()
                    {
                        ErrorCode = err,
                        Reason = reason
                    });
            };
            LibRdKafka.conf_set_error_cb(config, ErrorDelegate);

            if (logger == null)
            {
                logger = ((string handle, int level, string fac, string buf) =>
                {
                    var now = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
                    Console.WriteLine($"{level}|{now}|{handle}|{fac}| {buf}");
                });
            }

            LogDelegate = (IntPtr rk, int level, string fac, string buf) =>
            {
                // The log_cb is called very early during construction, before
                // SafeKafkaHandle or any of the C# wrappers are ready.
                // So we can't really pass rk on, just pass the rk name instead.
                var name = Marshal.PtrToStringAnsi(LibRdKafka.name(rk));
                logger(name, level, fac, buf);
            };
            LibRdKafka.conf_set_log_cb(config, LogDelegate);

            StatsDelegate = (IntPtr rk, IntPtr json, UIntPtr json_len, IntPtr opaque) =>
            {
                OnStatistics?.Invoke(this, Marshal.PtrToStringAnsi(json));
                return 0;
            };
            LibRdKafka.conf_set_stats_cb(config, StatsDelegate);

            handle = SafeKafkaHandle.Create(type, config);

            callbackCts = new CancellationTokenSource();
            callbackTask = StartCallbackTask(callbackCts.Token);
        }

        // TODO: Add timout parameter (with default option == block indefinitely) when use rd_kafka_flush.
        public void Flush()
        {
            // TODO: use rd_kafka_flush here instead..
            while (OutQueueLength > 0)
            {
                handle.Poll((IntPtr) 100);
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            // TODO: Is it possible that callbackCts and callbackTask have been collected when this is called by the finalizer?
            callbackCts.Cancel();
            callbackTask.Wait();

            if (disposing)
            {
                handle.Dispose();
            }
        }

        /// <summary>
        /// The name of the handle
        /// </summary>
        public string Name => handle.GetName();

        /// <summary>
        /// The client's broker-assigned group member id
        ///
        /// Last assigned member id, or empty string if not currently
        /// a group member.
        /// </summary>
        public string MemberId => handle.MemberId();

        /// <summary>
        /// The current out queue length
        ///
        /// The out queue contains messages and requests waiting to be sent to,
        /// or acknowledged by, the broker.
        /// </summary>
        public long OutQueueLength => handle.GetOutQueueLength();

        public int LogLevel
        {
            set {
                handle.SetLogLevel(value);
            }
        }

        /// <summary>
        /// Request Metadata from broker.
        ///
        /// Parameters:
        ///   allTopics    - if true: request info about all topics in cluster,
        ///                  if false: only request info about locally known topics.
        ///   onlyForTopic - only request info about this topic
        ///   includeInternal - include internal topics prefixed with __
        ///   timeout      - maximum response time before failing.
        /// </summary>
        public Task<Metadata> Metadata (bool allTopics=true, Topic onlyForTopic=null,
                bool includeInternal=false, TimeSpan timeout=default(TimeSpan))
            => Task.FromResult(handle.Metadata(allTopics, onlyForTopic?.handle, includeInternal, timeout));

        /// <summary>
        /// Request lowest and highest offsets for a topic partition from broker.
        /// </summary>
        public Task<Offsets> QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout=default(TimeSpan))
            => Task.FromResult(handle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, timeout));

        public struct ErrorArgs
        {
            public ErrorCode ErrorCode { get; set; }
            public string Reason { get; set; }
        }

        /// <summary>
        /// Fires on critical errors, e.g. connection failures or all brokers being down.
        /// </summary>
        public event EventHandler<ErrorArgs> OnError;

        public event EventHandler<string> OnStatistics;

        Task StartCallbackTask(CancellationToken ct)
            => Task.Factory.StartNew(() =>
                {
                    while (!ct.IsCancellationRequested)
                    {
                        handle.Poll((IntPtr) 1000);
                    }
                }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        public Task<List<GroupInfo>> ListGroups(TimeSpan timeout)
            => Task.FromResult(handle.ListGroups(null, (IntPtr) timeout.TotalMilliseconds));

        public Task<GroupInfo> ListGroup(string group, TimeSpan timeout)
            => Task.FromResult(handle.ListGroups(group, (IntPtr) timeout.TotalMilliseconds).Single());
    }
}
