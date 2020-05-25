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

namespace Confluent.Kafka.Benchmark
{
    /// <summary>
    /// Partitioner for benchmarking scenarios.
    /// This Partitioner will assign to Partition 0 by default,
    /// but can round-robin to all known partitions if configured.
    /// When Disposed the amount of times this was called will be
    /// written to the console.
    /// </summary>
    internal class BenchmarkPartioner : IPartitioner
    {
        // -1 due to a warmup call in produce benchmarks
        private int callCount = -1;
        private readonly bool useAllPartitions;

        public BenchmarkPartioner(bool useAllPartitions = false)
        {
            this.useAllPartitions = useAllPartitions;
        }

        public Partition Partition(string topic, IntPtr keydata, UIntPtr keylen, int partition_cnt, IntPtr rkt_opaque, IntPtr msg_opaque)
        {
            ++callCount;

            return this.useAllPartitions
                ? callCount % partition_cnt
                : 0;
        }

        public void Dispose()
        {
            Console.WriteLine($"{nameof(BenchmarkPartioner)} called {callCount} time(s) across {(useAllPartitions ? "all partitions" : "1 partition")}");
        }
    }
}
