// Copyright 2016-2020 Confluent Inc.
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

namespace Confluent.Kafka
{
    /// <summary>
    /// Partitioners will take over determining the partition of a message if no specific
    /// <seealso cref="Confluent.Kafka.Partition"/> is set (that is, <seealso cref="Confluent.Kafka.Partition.Any"/>).
    /// </summary>
    public interface IPartitioner : IDisposable
    {
        /// <summary>
        /// This method may be called in any thread at any time,
        /// it may be called multiple times for the same message/key.
        /// 
        /// Partitioner function constraints:
        ///   - MUST NOT call any rd_kafka_*() functions except:
        ///       rd_kafka_topic_partition_available()
        ///   - MUST NOT block or execute for prolonged periods of time.
        ///   - MUST return a value between 0 and partition_cnt-1, or the
        ///     special <seealso cref="Confluent.Kafka.Partition.Any"/> value if partitioning
        ///     could not be performed.
        /// </summary>
        /// <param name="topic">The topic to partition for.</param>
        /// <param name="keydata">Pointer to the Key's data.</param>
        /// <param name="keylen">The lenght of the Key's data.</param>
        /// <param name="partition_cnt">The known count of known partitions for the Topic.</param>
        /// <param name="rkt_opaque">The rkt_opaque argument is the opaque set by rd_kafka_topic_conf_set_opaque().</param>
        /// <param name="msg_opaque">The msg_opaque argument is the per-message opaque passed to produce().</param>
        /// <returns>The calculated <seealso cref="Confluent.Kafka.Partition"/> or <seealso cref="Confluent.Kafka.Partition.Any"/> if partitioning could not be performed.</returns>
        Partition Partition(string topic, IntPtr keydata, UIntPtr keylen, int partition_cnt, IntPtr rkt_opaque, IntPtr msg_opaque);
    }
}
