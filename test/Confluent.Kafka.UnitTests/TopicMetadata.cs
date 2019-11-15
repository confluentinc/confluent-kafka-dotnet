// Copyright 2016-2017 Confluent Inc.
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
using Xunit;


namespace Confluent.Kafka.UnitTests
{
    public class TopicMetadataTests
    {
        [Fact]
        public void Constuctor()
        {
            var partitions = new List<PartitionMetadata>();
            var tm = new TopicMetadata("mytopic", partitions, ErrorCode.Local_AllBrokersDown);

            Assert.Equal("mytopic", tm.Topic);
            Assert.Same(partitions, tm.Partitions);
            Assert.Equal(new Error(ErrorCode.Local_AllBrokersDown), tm.Error);
        }

        [Fact]
        public void ToStringTest()
        {
            int[] replicas = new int[]{1};
            int[] inSyncReplicas = new int[]{1};
            var pm1 = new PartitionMetadata(0, 1, replicas, inSyncReplicas, ErrorCode.NoError);
            var pm2 = new PartitionMetadata(1, 1, replicas, inSyncReplicas, ErrorCode.NoError);
            var partitions1 = new List<PartitionMetadata>{pm1, pm2};
            var tm1 = new TopicMetadata("mytopic", partitions1, ErrorCode.NoError); 

            Assert.Contains(tm1.Topic, tm1.ToString());
            Assert.Contains(pm1.ToString(), tm1.ToString());
            Assert.Contains(pm2.ToString(), tm1.ToString());

            var partitions2 = new List<PartitionMetadata>();
            var tm2 = new TopicMetadata("mytopic", partitions2, ErrorCode.Local_AllBrokersDown);
            Assert.Contains(tm2.Error.Code.ToString(), tm2.ToString());
        }
    }
}
