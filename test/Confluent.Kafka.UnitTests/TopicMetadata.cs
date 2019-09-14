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

        // TODO: ToString() tests. Note: there is coverage of this already in the Metadata integration test.
    }
}
