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

using Xunit;


namespace Confluent.Kafka.UnitTests
{
    public class PartitionMetadataTests
    {
        [Fact]
        public void Constuctor()
        {
            int[] replicas = new int[0];
            int[] inSyncReplicas = new int[0];
            var pm = new PartitionMetadata(33, 1, replicas, inSyncReplicas, ErrorCode.Local_AllBrokersDown);
            Assert.Equal(33, pm.PartitionId);
            Assert.Equal(1, pm.Leader);
            Assert.Same(replicas, pm.Replicas);
            Assert.Same(inSyncReplicas, pm.InSyncReplicas);
            Assert.Equal(new Error(ErrorCode.Local_AllBrokersDown), pm.Error);
        }

        [Fact]
        public void ToStringTest()
        {
            int[] replicas = new int[]{42, 43};
            int[] inSyncReplicas = new int[]{42};
            var pm1 = new PartitionMetadata(20, 42, replicas, inSyncReplicas, ErrorCode.NoError);

            Assert.Contains(pm1.PartitionId.ToString(), pm1.ToString());
            Assert.Contains(pm1.Error.Code.ToString(), pm1.ToString());
            foreach(var replica in pm1.Replicas)
            {
                Assert.Contains(replica.ToString(), pm1.ToString());
            }

            foreach(var inSyncReplica in pm1.InSyncReplicas)
            {
                Assert.Contains(inSyncReplica.ToString(), pm1.ToString());
            }


            int[] replicas2 = new int[0];
            int[] inSyncReplicas2 = new int[0];
            var pm2 = new PartitionMetadata(0, 41, replicas, inSyncReplicas, ErrorCode.NoError);

            Assert.Contains(pm2.Leader.ToString(), pm2.ToString());
        }
    }
}
