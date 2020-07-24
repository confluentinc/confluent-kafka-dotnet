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

using System.Threading;
using Xunit;

namespace Confluent.Kafka.UnitTests
{
    public class ProducerBuilderTests
    {
        [Fact]
        public void PartitionerTransfersToProducer()
        {
            var resetEvent = new ManualResetEventSlim(initialState: false);

            var producer = new ProducerBuilder<Null, Null>(new ProducerConfig())
                .SetKeySerializer(Serializers.Null)
                .SetPartitioner("test", new TestPartitioners.ResetOnDisposeTestPartitioner(resetEvent))
                .Build();

            Assert.False(resetEvent.IsSet);
            producer.Dispose(); // Partitioner, if set, will dispose when producer disposes, and Set the reset event
            Assert.True(resetEvent.IsSet);
        }
    }
}