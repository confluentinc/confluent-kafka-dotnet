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
using System.Threading;
using System.Collections.Generic;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Defines an Apache Kafka admin client.
    /// </summary>
    public interface IAdminClient : IClient
    {
        List<GroupInfo> ListGroups(TimeSpan timeout);
        
        GroupInfo ListGroup(string group, TimeSpan timeout);

        WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition);

        WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout);

        WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition);

        Metadata GetMetadata(bool allTopics, string topic, TimeSpan timeout);

        Metadata GetMetadata(bool allTopics, TimeSpan timeout);
    }
}
