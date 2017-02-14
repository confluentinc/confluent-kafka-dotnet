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


namespace Confluent.Kafka.Tests
{
    public class PartitionMetadataTests
    {
        [Fact]
        public void Constuctor()
        {
            int[] replicas = new int[0];
            int[] inSyncReplicas = new int[0];
            var pm = new PartitionMetadata(33, 1, replicas, inSyncReplicas, ErrorCode.Local_AllBrokersDown);
            Assert.Equal(pm.PartitionId, 33);
            Assert.Equal(pm.Leader, 1);
            Assert.Same(pm.Replicas, replicas);
            Assert.Same(pm.InSyncReplicas, inSyncReplicas);
            Assert.Equal(pm.Error, new Error(ErrorCode.Local_AllBrokersDown));
        }

        // TODO: ToString() tests. Note: there is coverage of this already in the Metdata integration test.
    }
}
