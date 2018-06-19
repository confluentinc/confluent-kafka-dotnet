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
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.


namespace Confluent.Kafka
{
    /// <summary>
    ///     Encapsulates the result of a produce request.
    /// </summary>
    public class DeliveryReport<TKey, TValue>
    {
        /// <summary>
        ///     The topic the message was produced to.
        /// </summary>
        public string Topic { get; set; }

        /// <summary>
        ///     The partition the message was produced to.
        /// </summary>
        public Partition Partition { get; set; }

        /// <summary>
        ///     The offset the message was produced to.
        /// </summary>
        public Offset Offset { get; set; }

        /// <summary>
        ///     An error (or NoError) associated with the produce request.
        /// </summary>
        public Error Error { get; set; }

        /// <summary>
        ///     The message that was produced.
        /// </summary>
        public Message<TKey, TValue> Message { get; set; }

        /// <summary>
        ///     The TopicPartition the message was produced to.
        /// </summary>
        public TopicPartition TopicPartition
            => new TopicPartition(Topic, Partition);

        /// <summary>
        ///     The TopicPartitionOffset associated with the produced message.
        /// </summary>
        public TopicPartitionOffset TopicPartitionOffset
            => new TopicPartitionOffset(Topic, Partition, Offset);

        /// <summary>
        ///     The TopicPartitionOffsetError assoicated with the produced message.
        /// </summary>
        public TopicPartitionOffsetError TopicPartitionOffsetError
        {
            get
            {
                return new TopicPartitionOffsetError(Topic, Partition, Offset, Error);
            }
            set
            {
                Topic = value.Topic;
                Partition = value.Partition;
                Offset = value.Offset;
                Error = value.Error;
            }
        }
    }
}
