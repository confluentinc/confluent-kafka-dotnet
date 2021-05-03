// Copyright 2016-2018 Confluent Inc.
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


namespace Confluent.Kafka
{
    /// <summary>
    ///     A high level producer with serialization capability.
    /// </summary>
    internal class Producer : ProducerBase, IProducer
    {
        internal Producer(DependentProducerBuilder builder) : base(builder)
        {
        }

        internal Producer(ProducerBuilder builder)
        {
            var baseConfig = builder.ConstructBaseConfig(this);
            Initialize(baseConfig);
        }

        /// <inheritdoc/>
        public Task<DeliveryResult> ProduceAsync(
            TopicPartition topicPartition,
            ReadOnlySpan<byte> key,
            ReadOnlySpan<byte> value,
            Headers headers = null,
            Timestamp timestamp = default,
            CancellationToken cancellationToken = default)
        {
            try
            {
                if (enableDeliveryReports)
                {
                    var handler = new TypedTaskDeliveryHandlerShim(topicPartition.Topic);

                    if (cancellationToken.CanBeCanceled)
                    {
                        handler.CancellationTokenRegistration
                            = cancellationToken.Register(() => handler.TrySetCanceled());
                    }

                    ProduceImpl(
                        topicPartition.Topic,
                        value,
                        key,
                        timestamp, topicPartition.Partition, headers,
                        handler);

                    return handler.Task;
                }
                else
                {
                    ProduceImpl(
                        topicPartition.Topic,
                        value,
                        key,
                        timestamp, topicPartition.Partition, headers,
                        null);

                    var result = new DeliveryResult
                    {
                        TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Unset),
                    };

                    return Task.FromResult(result);
                }
            }
            catch (KafkaException ex)
            {
                throw new ProduceException(
                    ex.Error,
                    new DeliveryResult
                    {
                        TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Unset)
                    });
            }
        }


        /// <inheritdoc/>
        public Task<DeliveryResult> ProduceAsync(
            string topic,
            ReadOnlySpan<byte> key,
            ReadOnlySpan<byte> value,
            Headers headers = null,
            Timestamp timestamp = default,
            CancellationToken cancellationToken = default)
            => ProduceAsync(
                new TopicPartition(topic, Partition.Any),
                key,
                value,
                headers,
                timestamp,
                cancellationToken);

        /// <inheritdoc/>
        public void Produce(
            string topic,
            ReadOnlySpan<byte> key,
            ReadOnlySpan<byte> value,
            Headers headers = null,
            Timestamp timestamp = default,
            Action<DeliveryReport> deliveryHandler = null)
            => Produce(
                new TopicPartition(topic, Partition.Any),
                key,
                value,
                headers,
                timestamp,
                deliveryHandler);


        /// <inheritdoc/>
        public void Produce(
            TopicPartition topicPartition,
            ReadOnlySpan<byte> key,
            ReadOnlySpan<byte> value,
            Headers headers = null,
            Timestamp timestamp = default,
            Action<DeliveryReport> deliveryHandler = null)
        {
            if (deliveryHandler != null && !enableDeliveryReports)
            {
                throw new InvalidOperationException("A delivery handler was specified, but delivery reports are disabled.");
            }

            try
            {
                ProduceImpl(
                    topicPartition.Topic,
                    value,
                    key,
                    timestamp, topicPartition.Partition,
                    headers,
                    new TypedDeliveryHandlerShim_Action(
                        topicPartition.Topic,
                        deliveryHandler));
            }
            catch (KafkaException ex)
            {
                throw new ProduceException(
                    ex.Error,
                    new DeliveryReport
                    {
                        TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Unset)
                    });
            }
        }

        private class TypedTaskDeliveryHandlerShim : TaskCompletionSource<DeliveryResult>, IDeliveryHandler
        {
            public TypedTaskDeliveryHandlerShim(string topic)
#if !NET45
                : base(TaskCreationOptions.RunContinuationsAsynchronously)
#endif
            {
                Topic = topic;
            }

            public CancellationTokenRegistration CancellationTokenRegistration;

            public string Topic;

            public void HandleDeliveryReport(DeliveryReport<Null, Null> deliveryReport)
            {
                if (CancellationTokenRegistration != null)
                {
                    CancellationTokenRegistration.Dispose();
                }

                if (deliveryReport == null)
                {
#if NET45
                    System.Threading.Tasks.Task.Run(() => TrySetResult(null));
#else
                    TrySetResult(null);
#endif
                    return;
                }

                var dr = new DeliveryResult
                {
                    TopicPartitionOffset = deliveryReport.TopicPartitionOffset,
                    Status = deliveryReport.Status,
                    Timestamp = deliveryReport.Message.Timestamp,
                    Headers = deliveryReport.Message.Headers
                };
                // topic is cached in this object, not set in the deliveryReport to avoid the 
                // cost of marshalling it.
                dr.Topic = Topic;

#if NET45
                if (deliveryReport.Error.IsError)
                {
                    System.Threading.Tasks.Task.Run(() => SetException(new ProduceException(deliveryReport.Error, dr)));
                }
                else
                {
                    System.Threading.Tasks.Task.Run(() => TrySetResult(dr));
                }
#else
                if (deliveryReport.Error.IsError)
                {
                    TrySetException(new ProduceException(deliveryReport.Error, dr));
                }
                else
                {
                    TrySetResult(dr);
                }
#endif
            }
        }

        private class TypedDeliveryHandlerShim_Action : IDeliveryHandler
        {
            public TypedDeliveryHandlerShim_Action(string topic, Action<DeliveryReport> handler)
            {
                Topic = topic;
                Handler = handler;
            }

            public string Topic;

            public Action<DeliveryReport> Handler;

            public void HandleDeliveryReport(DeliveryReport<Null, Null> deliveryReport)
            {
                if (deliveryReport == null)
                {
                    return;
                }

                var dr = new DeliveryReport
                {
                    TopicPartitionOffsetError = deliveryReport.TopicPartitionOffsetError,
                    Status = deliveryReport.Status,
                    Timestamp = deliveryReport.Message.Timestamp,
                    Headers = deliveryReport.Message.Headers
                };
                // topic is cached in this object, not set in the deliveryReport to avoid the 
                // cost of marshalling it.
                dr.Topic = Topic;

                if (Handler != null)
                {
                    Handler(dr);
                }
            }
        }
    }
}
