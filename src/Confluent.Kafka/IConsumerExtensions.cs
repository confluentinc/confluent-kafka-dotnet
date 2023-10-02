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

using System.Collections.Generic;
using System.Linq;


namespace Confluent.Kafka
{
    /// <summary>
    /// Common extension methods for <see cref="IConsumer{TKey, TValue}"/> implementations.
    /// </summary>
    public static class IConsumerExtensions
    {
        /// <summary>
        ///     Gets the current position (offset) for the
        ///     specified topic / partition.
        ///
        ///     The offset field of each requested partition
        ///     will be set to the offset of the last consumed
        ///     message + 1, or Offset.Unset in case there was
        ///     no previous message consumed by this consumer.
        ///     
        ///     The returned TopicPartitionOffset contains the leader epoch
        ///     too.
        /// </summary>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        public static TopicPartitionOffset PositionTopicPartitionOffset<TKey, TValue>(
            this IConsumer<TKey, TValue> consumer,
            TopicPartition partition)
        {
            try
            {
                return consumer.Handle.LibrdkafkaHandle.Position(new List<TopicPartition> { partition }).First();
            }
            catch (TopicPartitionOffsetException e)
            {
                throw new KafkaException(e.Results[0].Error);
            }
        }
    }
}
