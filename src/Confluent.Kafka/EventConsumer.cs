using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Confluent.Kafka
{
    /// <summary>
    /// Kafka Consumer that forwards received messages as events to the application.
    ///
    /// Thin abstraction on top of <see cref="Consumer" />.
    /// </summary>
    public class EventConsumer : Consumer
    {
        Task consumerTask;
        CancellationTokenSource consumerCts;

        public event EventHandler<Message> OnMessage;
        public event EventHandler<ErrorCode> OnConsumerError;
        public event EventHandler<TopicPartitionOffset> OnEndReached;

        public EventConsumer(IEnumerable<KeyValuePair<string,string>> config, IEnumerable<KeyValuePair<string, string>> defaultTopicConfig = null)
            : base(config, defaultTopicConfig)
        {}

        /// <summary>
        /// Start automatically consuming message and trigger events.
        ///
        /// Will invoke OnMessage, OnEndReached and OnConsumerError events.
        /// </summary>
        public void Start()
        {
            if (consumerTask != null)
            {
                throw new InvalidOperationException("Consumer task already running");
            }

            consumerCts = new CancellationTokenSource();
            var ct = consumerCts.Token;
            consumerTask = Task.Factory.StartNew(() =>
                {
                    while (!ct.IsCancellationRequested)
                    {
                        var messageAndError = Consume(TimeSpan.FromSeconds(1));
                        if (messageAndError.HasValue)
                        {
                            var mae = messageAndError.Value;
                            if (mae.Error == ErrorCode.NO_ERROR)
                            {
                                OnMessage?.Invoke(this, mae.Message);
                            }
                            else if (mae.Error == ErrorCode._PARTITION_EOF)
                            {
                                OnEndReached?.Invoke(this,
                                        new TopicPartitionOffset()
                                        {
                                            Topic = mae.Message.Topic,
                                            Partition = mae.Message.Partition,
                                            Offset = mae.Message.Offset,
                                        });
                            }
                            else
                            {
                                OnConsumerError?.Invoke(this, mae.Error);
                            }
                        }
                    }
                }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        public async Task Stop()
        {
            consumerCts.Cancel();
            try
            {
                await consumerTask;
            }
            finally
            {
                consumerTask = null;
                consumerCts = null;
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (consumerTask != null)
                {
                    Stop().Wait();
                }
            }

            base.Dispose(disposing);
        }
    }
}
